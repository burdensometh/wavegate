package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"wavegate/config"
	"wavegate/demo"
	"wavegate/journal"
	"wavegate/metrics"
	"wavegate/server"
	"wavegate/v1"
)

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usage()
	}
	switch args[0] {
	case "serve":
		return serve(ctx, args[1:])
	case "demo":
		flags := flag.NewFlagSet("demo", flag.ContinueOnError)
		fixtures := flags.String("fixtures", "samples/v1/demo-input.json", "v1 scenario file")
		output := flags.String("out", "", "new directory for the demo report and state")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *output == "" {
			return fmt.Errorf("usage: wavegate demo -out DIR [-fixtures JSON]")
		}
		return demo.Run(ctx, *fixtures, *output, os.Stdout)
	case "verify-receipt":
		return verify(args[0], args[1:], journal.VerifyReceipt)
	case "verify-window":
		return verify(args[0], args[1:], metrics.VerifyEvidence)
	case "inspect-state":
		return inspect(args[1:])
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage: wavegate {serve|demo|verify-receipt|verify-window|inspect-state} [flags]")
}

func inspect(args []string) (result error) {
	flags := flag.NewFlagSet("inspect-state", flag.ContinueOnError)
	path := flags.String("config", "", "v1 daemon configuration")
	directory := flags.String("state-directory", "", "override the configured state directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("usage: wavegate inspect-state [-config JSON] [-state-directory DIR]")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if *directory != "" {
		cfg.StateDirectory = *directory
	}
	state, err := os.Stat(cfg.StateDirectory)
	if err != nil {
		return err
	}
	if !state.IsDir() {
		return v1.Invalid("state path must be a directory")
	}
	app, err := server.Open(cfg, server.Options{})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, app.Close()) }()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(app.Recovery)
}

func verify[T any](name string, args []string, validate func(T) error) error {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	path := flags.String("input", "", "v1 JSON document, or - for stdin")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: wavegate %s -input FILE", name)
	}
	file := os.Stdin
	if *path != "-" {
		var err error
		file, err = os.Open(*path)
		if err != nil {
			return err
		}
		defer file.Close()
	}
	var doc T
	if err := v1.Decode(file, &doc); err != nil {
		return err
	}
	if err := validate(doc); err != nil {
		return err
	}
	fmt.Printf("%s OK\n", name)
	return nil
}

func serve(ctx context.Context, args []string) (result error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	path := flags.String("config", "", "v1 daemon configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("usage: wavegate serve [-config JSON]")
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	report := func(err error) { log.Printf("wavegate: %v", err) }
	app, err := server.Open(cfg, server.Options{Report: report})
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, app.Close()) }()
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Printf("API and traffic listening on http://%s", listener.Addr())
	return app.Serve(ctx, listener, report)
}
