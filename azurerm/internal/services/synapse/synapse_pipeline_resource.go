package synapse

import (
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/preview/synapse/2019-06-01-preview/artifacts"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"

	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/clients"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/synapse/parse"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/synapse/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/timeouts"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
)

func resourceSynapsePipeline() *schema.Resource {
	return &schema.Resource{
		Create: resourceSynapsePipelineCreateUpdate,
		Read:   resourceSynapsePipelineRead,
		Update: resourceSynapsePipelineCreateUpdate,
		Delete: resourceSynapsePipelineDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

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
				ValidateFunc: validate.SynapsePipelineAndTriggerName(),
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
				StateFunc:        utils.NormalizeJson,
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
	ctx, cancel := timeouts.ForCreate(meta.(*clients.Client).StopContext, d)
	defer cancel()
	environment := meta.(*clients.Client).Account.Environment

	workspaceId, err := parse.WorkspaceID(d.Get("synapse_workspace_id").(string))
	if err != nil {
		return err
	}

	pipelineName := d.Get("name").(string)
	client, err := synapseClient.PipelinesClient(workspaceId.Name, environment.SynapseEndpointSuffix)
	if err != nil {
		return err
	}

	log.Printf("[INFO] preparing arguments for Synapse Pipeline creation.")

	resourceGroupName := workspaceId.ResourceGroup
	name := pipelineName
	workspaceName := workspaceId.Name

	if d.IsNewResource() {
		existing, err := client.GetPipeline(ctx, pipelineName, "")
		if err != nil {
			if !utils.ResponseWasNotFound(existing.Response) {
				return fmt.Errorf("checking for presence of existing Synapse Pipeline %q (Resource Group %q / workspace %q): %s", name, resourceGroupName, workspaceName, err)
			}
		}

		if existing.ID != nil && *existing.ID != "" {
			return tf.ImportAsExistsError("azurerm_synapse_pipeline", *existing.ID)
		}
	}

	description := d.Get("description").(string)
	pipeline := &artifacts.Pipeline{
		Parameters:  expandSynapseParameters(d.Get("parameters").(map[string]interface{})),
		Variables:   expandSynapseVariables(d.Get("variables").(map[string]interface{})),
		Description: &description,
	}

	if v, ok := d.GetOk("activities_json"); ok {
		activities, err := deserializeSynapsePipelineActivities(v.(string))
		if err != nil {
			return fmt.Errorf("parsing 'activities_json' for Synapse Pipeline %q (Resource Group %q / Workspace %q) ID: %+v", name, resourceGroupName, workspaceName, err)
		}
		pipeline.Activities = activities
	}

	if v, ok := d.GetOk("annotations"); ok {
		annotations := v.([]interface{})
		pipeline.Annotations = &annotations
	} else {
		annotations := make([]interface{}, 0)
		pipeline.Annotations = &annotations
	}

	config := artifacts.PipelineResource{
		Pipeline: pipeline,
	}

	future, err := client.CreateOrUpdatePipeline(ctx, name, config, "")
	if err != nil {
		return fmt.Errorf("creating Synapse Pipeline %q (Resource Group %q / Workspace %q): %+v", name, resourceGroupName, workspaceName, err)
	}
	if err = future.WaitForCompletionRef(ctx, client.Client); err != nil {
		return fmt.Errorf("waiting on creation/updation for Synapse Pipeline %q (Resource Group %q / Workspace %q): %+v", name, resourceGroupName, workspaceName, err)
	}

	read, err := client.GetPipeline(ctx, name, "")
	if err != nil {
		return fmt.Errorf("retrieving Synapse Pipeline %q (Resource Group %q / Workspace %q): %+v", name, resourceGroupName, workspaceName, err)
	}

	if read.ID == nil {
		return fmt.Errorf("cannot read Synapse Pipeline %q (Resource Group %q / Workspace %q) ID", name, resourceGroupName, workspaceName)
	}

	d.SetId(*read.ID)

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
	name := id.Name

	resp, err := client.GetPipeline(ctx, name, "")
	if err != nil {
		if utils.ResponseWasNotFound(resp.Response) {
			d.SetId("")
			log.Printf("[DEBUG] Synapse Pipeline %q was not found - removing from state!", d.Id())
			return nil
		}
		return fmt.Errorf("reading the state of Synapse Pipeline %q: %+v", name, err)
	}

	workspaceId := parse.NewWorkspaceID(id.SubscriptionId, id.ResourceGroup, id.WorkspaceName).ID()
	d.Set("synapse_workspace_id", workspaceId)
	d.Set("name", resp.Name)

	if props := resp.Pipeline; props != nil {
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
	workspaceName := id.WorkspaceName
	name := id.Name
	resourceGroupName := id.ResourceGroup

	if _, err = client.DeletePipeline(ctx, name); err != nil {
		return fmt.Errorf("deleting Synapse Pipeline %q (Resource Group %q / Synapse %q): %+v", name, resourceGroupName, workspaceName, err)
	}

	return nil
}
