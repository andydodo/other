package util

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

func ParseConf(configPath string, out interface{}) (bool, error) {
	yamlFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		return false, err
	}
	err = yaml.UnmarshalStrict(yamlFile, out)

	if err != nil {
		return false, err
	}
	return true, nil
}
