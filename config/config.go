package config

import "github.com/spf13/viper"

type Config struct {
	BOOTSTRAP_SERVERS string `mapstructure:"BOOTSTRAP_SERVERS"`
	SECURITY_PROTOCOL string `mapstructure:"SECURITY_PROTOCOL"`
	SASL_MECHANISM    string `mapstructure:"SASL_MECHANISM"`
	SASL_USERNAME     string `mapstructure:"SASL_USERNAME"`
	SASL_PASSWORD     string `mapstructure:"SASL_PASSWORD"`
	ACKS              string `mapstructure:"ACKS"`
	TOPIC             string `mapstructure:"TOPIC"`
	GROUP_ID          string `mapstructure:"GROUP_ID"`
	AUTO_OFFSET_RESET string `mapstructure:"AUTO_OFFSET_RESET"`
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(path string) (config Config, err error) {
	viper.SetConfigFile(path)  // name of config file (without extension)
	viper.SetConfigType("env") // format of config file (yaml, json, properties, etc.)
	err = viper.ReadInConfig() // Find and read the config file
	if err != nil {            // Handle errors reading the config file
		return
	}

	err = viper.Unmarshal(&config) // Unmarshal the config into a struct
	return
}
