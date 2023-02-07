package main

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wesen/plunger/pkg"
	"os"
	"strings"
	"time"
)

func initViper(appName string, configFilePath string) error {
	viper.SetEnvPrefix(appName)

	if configFilePath != "" {
		viper.SetConfigFile(configFilePath)
	} else {
		viper.AddConfigPath(".")
		viper.AddConfigPath(fmt.Sprintf("$HOME/.%s", appName))
		viper.AddConfigPath(fmt.Sprintf("/etc/%s", appName))

		xdgConfigPath, err := os.UserConfigDir()
		if err == nil {
			viper.AddConfigPath(fmt.Sprintf("%s/%s", xdgConfigPath, appName))
		}
	}

	// Read the configuration file into Viper
	err := viper.ReadInConfig()
	// if the file does not exist, continue normally
	if _, ok := err.(viper.ConfigFileNotFoundError); ok {
		// Config file not found; ignore error
	} else if err != nil {
		// Config file was found but another error was produced
		return err
	}
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Bind the variables to the command-line flags
	err = viper.BindPFlags(rootCmd.PersistentFlags())
	if err != nil {
		return err
	}

	return nil
}

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Log a message",
	Run: func(cmd *cobra.Command, args []string) {
		force, _ := cmd.Flags().GetBool("force")

		metaKeys, _ := cmd.Flags().GetStringSlice("meta-keys")

		schema := pkg.NewSchema()

		for _, metaKey := range metaKeys {
			schema.MetaKeys.Add(metaKey)
		}

		logWriter, err := initConfigAndLogging(force, schema)
		cobra.CheckErr(err)

		defer func(logWriter *pkg.LogWriter) {
			err := logWriter.Close()
			if err != nil {
				fmt.Println(err)
			}
		}(logWriter)

		log.Debug().Msg("hello world")
		log.Info().
			Int("int", 32).
			Ints("ints", []int{1, 2, 3}).
			Str("string", "hello").
			Time("time", time.Now()).
			Msg("hello world")
	},
}

func initConfigAndLogging(deleteFile bool, schema *pkg.Schema) (*pkg.LogWriter, error) {
	err := initViper("plunger", "")
	cobra.CheckErr(err)

	logLevel := viper.GetString("log-level")
	verbose := viper.GetBool("verbose")
	if verbose && logLevel != "trace" {
		logLevel = "debug"
	}

	config := &pkg.LoggerConfig{
		WithCaller: viper.GetBool("with-caller"),
		Level:      logLevel,
		DBFile:     viper.GetString("db"),
		Schema:     schema,
	}

	if deleteFile {
		err = os.Remove(config.DBFile)
		if err != nil {
			fmt.Println(err)
		}
	}

	logWriter, _, err := pkg.InitLogging(config)
	cobra.CheckErr(err)

	return logWriter, err
}

var rootCmd = &cobra.Command{
	Use: "plunger",
}

func init() {
	rootCmd.PersistentFlags().String("db", "", "Database file")

	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Bool("force", false, "Delete the log file before starting")
	logCmd.Flags().StringSlice("meta-keys", []string{}, "Meta keys")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
