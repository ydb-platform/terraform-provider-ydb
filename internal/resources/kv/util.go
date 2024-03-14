package kv

import (
	"errors"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func expandStorageConfig(d *schema.ResourceData) (*ChannelConfig, error) {
	channel_config := &ChannelConfig{}
	stcfg, ok := d.Get("storage_config").([]interface{})
	if !ok {
		return nil, errors.New("can't parse storage_config")
	}
	chancfg, ok := stcfg[0].(map[string]interface{})
	if !ok {
		return nil, errors.New("can't parse channel_config")
	}
	for _, v := range chancfg["channel"].([]interface{}) {
		med, ok := v.(map[string]interface{})
		if !ok {
			return nil, errors.New("can't parse media")
		}
		sttype, ok := med["media"].(string)
		if !ok {
			return nil, errors.New("wrong media")
		}
		channel_config.Channel = append(channel_config.Channel, &MediaConfig{Media: sttype})
	}
	return channel_config, nil
}