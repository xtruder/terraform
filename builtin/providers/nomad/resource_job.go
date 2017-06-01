package nomad

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/nomad/jobspec"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/terraform/helper/schema"
)

func resourceJob() *schema.Resource {
	return &schema.Resource{
		Create: resourceJobRegister,
		Update: resourceJobRegister,
		Delete: resourceJobDeregister,
		Read:   resourceJobRead,

		CustomizeDiff: resourceJobCustomizeDiff,

		Schema: map[string]*schema.Schema{
			"jobspec": {
				Description:      "Job specification. If you want to point to a file use the file() function.",
				Required:         true,
				Type:             schema.TypeString,
				DiffSuppressFunc: jobspecDiffSuppress,
			},

			"deregister_on_destroy": {
				Description: "If true, the job will be deregistered on destroy.",
				Optional:    true,
				Default:     true,
				Type:        schema.TypeBool,
			},

			"id": {
				Description: "Id of the job itself.",
				Computed:    true,
				Type:        schema.TypeString,
			},

			"name": {
				Description: "Name of the job.",
				Computed:    true,
				Type:        schema.TypeString,
			},

			"evaluation_id": {
				Description: "Id of the current evaluation for the job.",
				Computed:    true,
				Type:        schema.TypeString,
			},

			"job_modify_index": {
				Description: "Incrementing number used to identify a particular modification of the job.",
				Computed:    true,
				Type:        schema.TypeString, // this is a uint64 in Nomad, so we can't use TypeInt
			},

			// Remaining computed fields are exposed primarily to expose the
			// Nomad diff in the Terraform plan, but can also potentially
			// be used for interpolations.
			// At present this isn't fully comprehensive, just cherry-picking
			// a few fields that are likely to be interesting. Fully
			// representing the Nomad structure here would be nice, but
			// is a non-trivial amount of work, so we'll consider this subset
			// better than what we had before, which was nothing at all.

			"region": {
				Computed: true,
				Type:     schema.TypeString,
			},

			"priority": {
				Computed: true,
				Type:     schema.TypeInt,
			},

			"datacenters": {
				Computed: true,
				Type:     schema.TypeList,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},

			"task_groups": {
				Computed: true,
				Type:     schema.TypeList,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Description: "Name of the task group.",
							Computed:    true,
							Type:        schema.TypeString,
						},
						"count": {
							Computed: true,
							Type:     schema.TypeInt,
						},
						"meta": {
							Computed: true,
							Type:     schema.TypeMap,
						},

						"tasks": {
							Computed: true,
							Type:     schema.TypeList,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Computed: true,
										Type:     schema.TypeString,
									},
									"driver": {
										Computed: true,
										Type:     schema.TypeString,
									},
									"config_json": {
										Computed: true,
										Type:     schema.TypeString,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceJobCustomizeDiff(d *schema.ResourceDiff, meta interface{}) error {
	client := meta.(*api.Client)

	if !d.HasChange("jobspec") {
		// Nothing to do!
		return nil
	}

	// Get the jobspec itself
	jobspecRaw := d.Get("jobspec").(string)
	jobspecAPI, err := prepareJobSpec(jobspecRaw)
	if err != nil {
		return err
	}

	log.Printf("[DEBUG] Generating plan for job")
	resp, _, err := client.Jobs().Plan(jobspecAPI, true, nil)
	if err != nil {
		return fmt.Errorf("error from nomad plan: %s", err)
	}

	d.SetNew("id", jobspecAPI.ID)
	d.SetNew("name", jobspecAPI.Name)

	if d.HasChange("id") {
		d.ForceNew("id")
	}
	if d.HasChange("name") {
		d.ForceNew("name")
	}

	// Each change creates a new evaluation.
	d.SetNewComputed("evaluation_id")

	// After we apply, the job_modify_index will have been incremented, but
	// we don't know by how much because registering a job can potentially
	// make many small changes.
	d.SetNewComputed("job_modify_index")

	nomadDiff := resp.Diff

	// For dev/debug
	log.Printf("[DEBUG] nomad job diff: %s", spew.Sdump(resp.Diff))

	for _, fd := range nomadDiff.Fields {
		switch fd.Name {
		case "Region":
			d.SetNew("region", fd.New)
		}
	}
	for _, od := range nomadDiff.Objects {
		switch od.Name {
		case "Datacenters":
			dcs := d.Get("datacenters").([]interface{})
			for _, fd := range od.Fields {
				switch fd.Type {
				case "Added":
					dcs = append(dcs, fd.New)
				case "Deleted":
					for i := range dcs {
						if dcs[i].(string) == fd.Old {
							// Shift up everything after this in the slice
							// to close the gap
							copy(dcs[i:], dcs[i+1:])
							dcs = dcs[:len(dcs)-1]
							break
						}
					}
				}
			}
			d.SetNew("datacenters", dcs)
		}
	}

	tgMaps := d.Get("task_groups").([]interface{})
	for _, tg := range nomadDiff.TaskGroups {
		var tgMap map[string]interface{}
		for _, tgmI := range tgMaps {
			tgm := tgmI.(map[string]interface{})
			if tgm["name"] == tg.Name {
				tgMap = tgm
			}
		}
		if tgMap == nil {
			tgMap = map[string]interface{}{
				"name": tg.Name,
			}
			tgMaps = append(tgMaps, tgMap)
		}

		for _, fd := range tg.Fields {
			switch fd.Name {
			case "Count":
				num, _ := strconv.Atoi(fd.New)
				tgMap["count"] = num
			}
		}

		// FIXME: Also need to take care of task diffs
	}
	d.SetNew("task_groups", tgMaps)

	return nil
}

func resourceJobRegister(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*api.Client)

	// Get the jobspec itself
	jobspecRaw := d.Get("jobspec").(string)
	jobspecAPI, err := prepareJobSpec(jobspecRaw)
	if err != nil {
		return err
	}

	oldModifyIndexI, _ := d.GetChange("job_modify_index")
	oldModifyIndexS := oldModifyIndexI.(string)
	var oldModifyIndex uint64
	if oldModifyIndexS == "" {
		oldModifyIndex = 0
	} else {
		var err error
		oldModifyIndex, err = strconv.ParseUint(oldModifyIndexS, 10, 64)
		if err != nil {
			// should never happen unless the plan was tampered with
			return fmt.Errorf("invalid old job_modify_index: %s", err)
		}
	}

	// Register the job
	evalID, _, err := client.Jobs().EnforceRegister(jobspecAPI, oldModifyIndex, nil)
	if err != nil {
		return fmt.Errorf("error applying jobspec: %s", err)
	}

	d.SetId(jobspecAPI.ID)
	d.Set("evaluation_id", evalID)

	return resourceJobRead(d, meta)
}

func resourceJobDeregister(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*api.Client)

	// If deregistration is disabled, then do nothing
	if !d.Get("deregister_on_destroy").(bool) {
		log.Printf(
			"[WARN] Job %q will not deregister since 'deregister_on_destroy'"+
				" is false", d.Id())
		return nil
	}

	id := d.Id()
	log.Printf("[DEBUG] Deregistering job: %q", id)
	_, _, err := client.Jobs().Deregister(id, nil)
	if err != nil {
		return fmt.Errorf("error deregistering job: %s", err)
	}

	return nil
}

func resourceJobRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*api.Client)

	id := d.Id()
	log.Printf("[DEBUG] Reading job %q", id)
	job, _, err := client.Jobs().Info(id, nil)
	if err != nil {
		// As of Nomad 0.4.1, the API client returns an error for 404
		// rather than a nil result, so we must check this way.
		if strings.Contains(err.Error(), "404") {
			log.Printf("[DEBUG] job %q no longer exists", id)
			return nil
		}

		return fmt.Errorf("error checking for job: %#v", err)
	}

	log.Printf("[DEBUG] Job from Nomad API: %s", spew.Sdump(job))

	jmi := job.JobModifyIndex

	d.Set("name", job.Name)
	d.Set("job_modify_index", strconv.FormatUint(jmi, 10))
	d.Set("region", job.Region)
	d.Set("priority", job.Priority)
	d.Set("datacenters", job.Datacenters)

	tgMaps := make([]interface{}, len(job.TaskGroups))
	for i, tg := range job.TaskGroups {
		tgMap := map[string]interface{}{
			"name":  tg.Name,
			"count": tg.Count,
			"meta":  tg.Meta,
		}

		tMaps := make([]interface{}, len(tg.Tasks))
		for j, t := range tg.Tasks {
			configJSON, _ := json.Marshal(t.Config)

			tMap := map[string]interface{}{
				"name":        t.Name,
				"driver":      t.Driver,
				"config_json": string(configJSON),
			}
			tMaps[j] = tMap
		}

		tgMap["tasks"] = tMaps
		tgMaps[i] = tgMap
	}
	d.Set("task_groups", tgMaps)

	return nil
}

// prepareJobSpec attempts to turn a job specification string (HCL syntax)
// into a job object ready to submit to the Nomad API.
func prepareJobSpec(raw string) (*api.Job, error) {
	// Parse it
	jobspecStruct, err := jobspec.Parse(strings.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("error parsing jobspec: %s", err)
	}

	// Initialize and validate
	jobspecStruct.Canonicalize()
	if err := jobspecStruct.Validate(); err != nil {
		return nil, fmt.Errorf("Error validating job: %v", err)
	}

	// Convert it so that we can use it with the API
	jobspecAPI, err := convertStructJob(jobspecStruct)
	if err != nil {
		return nil, fmt.Errorf("error converting jobspec: %s", err)
	}

	return jobspecAPI, nil
}

// convertStructJob is used to take a *structs.Job and convert it to an *api.Job.
//
// This is unfortunate but it is how Nomad itself does it (this is copied
// line for line from Nomad). We'll mimic them exactly to get this done.
func convertStructJob(in *structs.Job) (*api.Job, error) {
	gob.Register([]map[string]interface{}{})
	gob.Register([]interface{}{})
	var apiJob *api.Job
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(in); err != nil {
		return nil, err
	}
	if err := gob.NewDecoder(buf).Decode(&apiJob); err != nil {
		return nil, err
	}
	return apiJob, nil
}

// jobspecDiffSuppress is the DiffSuppressFunc used by the schema to
// check if two jobspecs are equal.
func jobspecDiffSuppress(k, old, new string, d *schema.ResourceData) bool {
	// Parse the old job
	oldJob, err := jobspec.Parse(strings.NewReader(old))
	if err != nil {
		return false
	}

	// Parse the new job
	newJob, err := jobspec.Parse(strings.NewReader(new))
	if err != nil {
		return false
	}

	// Init
	oldJob.Canonicalize()
	newJob.Canonicalize()

	// Check for jobspec equality
	return reflect.DeepEqual(oldJob, newJob)
}
