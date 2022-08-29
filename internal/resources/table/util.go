package table

import "github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"

func GetTokenFromResource(d *schema.ResourceData) string {
	tok, ok := d.GetOk("token")
	if !ok {
		return ""
	}
	return tok.(string)
}
