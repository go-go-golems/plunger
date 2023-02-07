package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
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

type logConfig struct {
	WithCaller bool
	Level      string
	DBFile     string
}

type MissingDBFileError struct {
}

func (e *MissingDBFileError) Error() string {
	return "missing db file"
}

func initLogging(config *logConfig) (*pkg.LogWriter, *sqlx.DB, error) {
	logLevel := viper.GetString("log-level")
	verbose := viper.GetBool("verbose")
	if verbose && logLevel != "trace" {
		logLevel = "debug"
	}

	if config.WithCaller {
		log.Logger = log.With().Caller().Logger()
	}

	if config.DBFile == "" {
		return nil, nil, &MissingDBFileError{}
	}

	db, err := sqlx.Open("sqlite3", config.DBFile)
	if err != nil {
		return nil, nil, err
	}

	logWriter := pkg.NewLogWriter(db, nil)
	err = logWriter.Init()
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	log.Logger = log.Output(logWriter)

	switch config.Level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	}

	return logWriter, db, nil
}

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Log a message",
	Run: func(cmd *cobra.Command, args []string) {
		err := initViper("plunger", "")
		cobra.CheckErr(err)

		config := &logConfig{
			WithCaller: viper.GetBool("with-caller"),
			Level:      viper.GetString("log-level"),
			DBFile:     viper.GetString("db"),
		}
		logWriter, _, err := initLogging(config)
		defer func(logWriter *pkg.LogWriter) {
			err := logWriter.Close()
			if err != nil {
				fmt.Println(err)
			}
		}(logWriter)

		cobra.CheckErr(err)

		log.Debug().Msg("hello world")
		log.Info().
			Int("int", 32).
			Ints("ints", []int{1, 2, 3}).
			Str("string", "hello").
			Time("time", time.Now()).
			Msg("hello world")
	},
}

var rootCmd = &cobra.Command{
	Use: "plunger",
}

func init() {
	rootCmd.PersistentFlags().String("db", "", "Database file")

	rootCmd.AddCommand(logCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
