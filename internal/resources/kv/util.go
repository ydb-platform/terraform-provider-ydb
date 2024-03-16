package kv

import (
	"errors"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

func expandStorageConfig(d *schema.ResourceData) (*ChannelConfig, error) {
	channelConfig := &ChannelConfig{}
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
		channelConfig.Channel = append(channelConfig.Channel, &MediaConfig{Media: sttype})
	}
	return channelConfig, nil
}
