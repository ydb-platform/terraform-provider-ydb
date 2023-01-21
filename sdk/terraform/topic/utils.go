package topic

import (
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func defaultTimeouts() *schema.ResourceTimeout {
	return &schema.ResourceTimeout{
		Create:  schema.DefaultTimeout(time.Minute * 20),
		Read:    schema.DefaultTimeout(time.Minute * 20),
		Update:  schema.DefaultTimeout(time.Minute * 20),
		Delete:  schema.DefaultTimeout(time.Minute * 20),
		Default: schema.DefaultTimeout(time.Minute * 20),
	}
}
