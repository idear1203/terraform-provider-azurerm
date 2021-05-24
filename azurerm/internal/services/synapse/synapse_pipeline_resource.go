package synapse

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/go-autorest/autorest"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/helper/validation"

	"github.com/Azure/azure-sdk-for-go/sdk/synapse/azartifacts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/synapse/parse"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/synapse/validate"
	azSchema "github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/tf/schema"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceSynapsePipeline() *schema.Resource {
	return &schema.Resource{
		Create: resourceSynapsePipelineCreateUpdate,
		Read:   resourceSynapsePipelineRead,
		Update: resourceSynapsePipelineCreateUpdate,
		Delete: resourceSynapsePipelineDelete,
		Importer: azSchema.ValidateResourceIDPriorToImport(func(id string) error {
			_, err := parse.PipelineID(id)
			return err
		}),

		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(30 * time.Minute),
			Read:   schema.DefaultTimeout(5 * time.Minute),
			Update: schema.DefaultTimeout(30 * time.Minute),
			Delete: schema.DefaultTimeout(30 * time.Minute),
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.PipelineAndTriggerName(),
			},

			"synapse_workspace_id": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validate.WorkspaceID,
			},

			"parameters": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},

			"variables": {
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},

			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},

			"activities_json": {
				Type:             schema.TypeString,
				Optional:         true,
				ValidateFunc:     validation.StringIsJSON,
				DiffSuppressFunc: suppressJsonOrderingDifference,
			},

			"annotations": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func resourceSynapsePipelineCreateUpdate(d *schema.ResourceData, meta interface{}) error {
	synapseClient := meta.(*clients.Client).Synapse
	ctx, cancel := timeouts.ForCreateUpdate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	environment := meta.(*clients.Client).Account.Environment

	workspaceId, err := parse.WorkspaceID(d.Get("synapse_workspace_id").(string))
	if err != nil {
		return err
	}

	client, err := synapseClient.PipelinesClient(workspaceId.Name, environment.SynapseEndpointSuffix)
	if err != nil {
		return err
	}

	log.Printf("[INFO] preparing arguments for Synapse Pipeline creation.")

	id := parse.NewPipelineID(workspaceId.SubscriptionId, workspaceId.ResourceGroup, workspaceId.Name, d.Get("name").(string))
	if d.IsNewResource() {
		existing, err := client.GetPipeline(ctx, id.Name, nil)
		if err != nil {
			if !utils.ResponseWasNotFound(convertResponse(err)) {
				return fmt.Errorf("checking for present of existing %s: %+v", id, err)
			}
		}

		if err == nil && !utils.ResponseWasNotFound(autorest.Response{Response: existing.RawResponse}) {
			return tf.ImportAsExistsError("azurerm_synapse_pipeline", id.ID())
		}
	}

	pipeline := &azartifacts.Pipeline{
		Parameters:  *expandSynapseParameters(d.Get("parameters").(map[string]interface{})),
		Variables:   *expandSynapseVariables(d.Get("variables").(map[string]interface{})),
		Description: utils.String(d.Get("description").(string)),
	}

	if v, ok := d.GetOk("activities_json"); ok {
		activities, err := deserializeSynapsePipelineActivities(v.(string))
		if err != nil {
			return fmt.Errorf("parsing 'activities_json' for %s", err)
		}
		pipeline.Activities = activities
	}

	if v, ok := d.GetOk("annotations"); ok {
		annotations := v.([]interface{})
		pipeline.Annotations = annotations
	} else {
		annotations := make([]interface{}, 0)
		pipeline.Annotations = annotations
	}

	config := azartifacts.PipelineResource{
		Properties: pipeline,
	}

	resp, err := client.BeginCreateOrUpdatePipeline(ctx, id.Name, config, nil)
	if err != nil {
		return fmt.Errorf("creating %s: %+v", id, err)
	}
	if _, err = resp.PollUntilDone(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("waiting on creation/updation for %s: %+v", id, err)
	}

	d.SetId(id.ID())

	return resourceSynapsePipelineRead(d, meta)
}

func resourceSynapsePipelineRead(d *schema.ResourceData, meta interface{}) error {
	synapseClient := meta.(*clients.Client).Synapse
	ctx, cancel := timeouts.ForRead(meta.(*clients.Client).StopContext, d)
	defer cancel()
	environment := meta.(*clients.Client).Account.Environment

	id, err := parse.PipelineID(d.Id())
	if err != nil {
		return err
	}

	client, err := synapseClient.PipelinesClient(id.WorkspaceName, environment.SynapseEndpointSuffix)
	if err != nil {
		return err
	}

	resp, err := client.GetPipeline(ctx, id.Name, nil)
	if err != nil {
		if utils.ResponseWasNotFound(convertResponse(err)) {
			d.SetId("")
			log.Printf("[DEBUG] Synapse Pipeline %q was not found - removing from state!", d.Id())
			return nil
		}
		return fmt.Errorf("reading %s: %+v", id, err)
	}

	workspaceId := parse.NewWorkspaceID(id.SubscriptionId, id.ResourceGroup, id.WorkspaceName).ID()
	d.Set("synapse_workspace_id", workspaceId)
	d.Set("name", id.Name)

	if props := resp.PipelineResource.Properties; props != nil {
		d.Set("description", props.Description)

		parameters := flattenSynapseParameters(props.Parameters)
		if err := d.Set("parameters", parameters); err != nil {
			return fmt.Errorf("setting `parameters`: %+v", err)
		}

		annotations := flattenSynapseAnnotations(props.Annotations)
		if err := d.Set("annotations", annotations); err != nil {
			return fmt.Errorf("setting `annotations`: %+v", err)
		}

		variables := flattenSynapseVariables(props.Variables)
		if err := d.Set("variables", variables); err != nil {
			return fmt.Errorf("setting `variables`: %+v", err)
		}

		if activities := props.Activities; activities != nil {
			activitiesJson, err := serializeSynapsePipelineActivities(activities)
			if err != nil {
				return fmt.Errorf("serializing `activities_json`: %+v", err)
			}
			if err := d.Set("activities_json", activitiesJson); err != nil {
				return fmt.Errorf("setting `activities_json`: %+v", err)
			}
		}
	}

	return nil
}

func resourceSynapsePipelineDelete(d *schema.ResourceData, meta interface{}) error {
	synapseClient := meta.(*clients.Client).Synapse
	ctx, cancel := timeouts.ForDelete(meta.(*clients.Client).StopContext, d)
	defer cancel()
	environment := meta.(*clients.Client).Account.Environment

	id, err := parse.PipelineID(d.Id())
	if err != nil {
		return err
	}

	client, err := synapseClient.PipelinesClient(id.WorkspaceName, environment.SynapseEndpointSuffix)
	if err != nil {
		return err
	}

	resp, err := client.BeginDeletePipeline(ctx, id.Name, nil)
	if err != nil {
		return fmt.Errorf("deleting %s: %+v", id, err)
	}
	if _, err = resp.PollUntilDone(ctx, 30*time.Second); err != nil {
		return fmt.Errorf("waiting on deletion for %s: %+v", id, err)
	}

	return nil
}

func expandSynapseParameters(input map[string]interface{}) *map[string]*azartifacts.ParameterSpecification {
	output := make(map[string]*azartifacts.ParameterSpecification)

	for k, v := range input {
		output[k] = &azartifacts.ParameterSpecification{
			Type:         azartifacts.ParameterTypeString.ToPtr(),
			DefaultValue: v.(string),
		}
	}

	return &output
}

func flattenSynapseParameters(input map[string]*azartifacts.ParameterSpecification) map[string]interface{} {
	output := make(map[string]interface{})

	for k, v := range input {
		if v != nil {
			// we only support string parameters at this time
			val, ok := v.DefaultValue.(string)
			if !ok {
				log.Printf("[DEBUG] Skipping parameter %q since it's not a string", k)
			}

			output[k] = val
		}
	}

	return output
}

func flattenSynapseAnnotations(input []interface{}) []string {
	annotations := make([]string, 0)
	if input == nil {
		return annotations
	}

	for _, annotation := range input {
		val, ok := annotation.(string)
		if !ok {
			log.Printf("[DEBUG] Skipping annotation %q since it's not a string", val)
		}
		annotations = append(annotations, val)
	}
	return annotations
}

func expandSynapseVariables(input map[string]interface{}) *map[string]*azartifacts.VariableSpecification {
	output := make(map[string]*azartifacts.VariableSpecification)

	for k, v := range input {
		output[k] = &azartifacts.VariableSpecification{
			Type:         azartifacts.VariableTypeString.ToPtr(),
			DefaultValue: v.(string),
		}
	}

	return &output
}

func flattenSynapseVariables(input map[string]*azartifacts.VariableSpecification) map[string]interface{} {
	output := make(map[string]interface{})

	for k, v := range input {
		if v != nil {
			// we only support string parameters at this time
			val, ok := v.DefaultValue.(string)
			if !ok {
				log.Printf("[DEBUG] Skipping variable %q since it's not a string", k)
			}

			output[k] = val
		}
	}

	return output
}

func deserializeSynapsePipelineActivities(jsonData string) ([]azartifacts.ActivityClassification, error) {
	jsonData = fmt.Sprintf(`{ "properties": { "activities": %s }}`, jsonData)
	pipelineResource := &azartifacts.PipelineResource{}
	err := pipelineResource.UnmarshalJSON([]byte(jsonData))
	if err != nil {
		return nil, err
	}
	return pipelineResource.Properties.Activities, nil
}

func serializeSynapsePipelineActivities(activities []azartifacts.ActivityClassification) (string, error) {
	pipeline := &azartifacts.Pipeline{Activities: activities}
	pipelineResource := &azartifacts.PipelineResource{Properties: pipeline}
	result, err := pipelineResource.MarshalJSON()
	if err != nil {
		return "nil", err
	}

	var m map[string]map[string]*json.RawMessage
	err = json.Unmarshal(result, &m)
	if err != nil {
		return "", err
	}

	activitiesJson, err := json.Marshal(m["properties"]["activities"])
	if err != nil {
		return "", err
	}

	return string(activitiesJson), nil
}

func suppressJsonOrderingDifference(_, old, new string, _ *schema.ResourceData) bool {
	return utils.NormalizeJson(old) == utils.NormalizeJson(new)
}

func convertResponse(err error) autorest.Response {
	return autorest.Response{Response: err.(azcore.HTTPResponse).RawResponse()}
}
