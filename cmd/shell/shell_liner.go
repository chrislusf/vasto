package shell

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/peterh/liner"
)

var (
	line        *liner.State
	historyPath = "/tmp/vasto-shell"
)

func (s *shell) runShell() {

	line = liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	setCompletionHandler()
	loadHisotry()

	defer saveHisotry()

	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	commandEnv := &CommandEnv{
		keyspace: *s.option.Keyspace,
	}
	if commandEnv.keyspace != "" {
		commandEnv.clusterClient = s.vastoClient.NewClusterClient(commandEnv.keyspace)
	}

	for {
		cmd, err := line.Prompt("> ")
		if err != nil {
			fmt.Printf("%v\n", err)
			return
		}

		cmds := reg.FindAllString(cmd, -1)
		if len(cmds) == 0 {
			continue
		} else {
			line.AppendHistory(cmd)

			env := make(map[string]string)
			envIndex := 0
			for envIndex = range cmds {
				if strings.Contains(cmds[envIndex], "=") {
					kv := strings.SplitN(cmds[envIndex], "=", 2)
					commandEnv.env[kv[0]] = kv[1]
				} else {
					break
				}
			}
			commandEnv.env = env
			cmds = cmds[envIndex:]

			args := make([]string, len(cmds[1:]))

			for i := range args {
				args[i] = strings.Trim(string(cmds[1+i]), "\"'")
			}

			cmd := strings.ToLower(cmds[0])
			if cmd == "help" || cmd == "?" {
				printHelp(cmds)
			} else if cmd == "exit" || cmd == "quit" {
				return
			} else {
				for _, c := range commands {
					if c.Name() == cmd {
						err := parseEnv(commandEnv.clusterClient, commandEnv.env)
						if err != nil {
							fmt.Println()
							fmt.Printf("\t%s %v \n", c.Name(), err)
							fmt.Println()
							continue
						}
						if err := c.Do(s.vastoClient, args, commandEnv, os.Stderr); err != nil {
							fmt.Fprintf(os.Stderr, "error: %v\n", err)
							if err == InvalidArguments {
								fmt.Println()
								fmt.Printf("\t%s %s \n", c.Name(), c.Help())
								fmt.Println()
							}
						}
					}
				}
			}

		}
	}
}

func printGenericHelp() {
	msg :=
		`vasto shell
Type:	"help <command>" for help on <command>
`
	fmt.Print(msg)
	for _, c := range commands {
		fmt.Printf("\t%s %s \n", c.Name(), c.Help())
	}
}

func printHelp(cmds []string) {
	args := cmds[1:]
	if len(args) == 0 {
		printGenericHelp()
	} else if len(args) > 1 {
		fmt.Println()
	} else {
		cmd := strings.ToLower(args[0])
		for _, c := range commands {
			if c.Name() == cmd {
				fmt.Println()
				fmt.Printf("\t%s %s \n", c.Name(), c.Help())
				fmt.Println()
			}
		}
	}
}

func setCompletionHandler() {
	line.SetCompleter(func(line string) (c []string) {
		for _, i := range commands {
			if strings.HasPrefix(i.Name(), strings.ToLower(line)) {
				c = append(c, i.Name())
			}
		}
		return
	})
}

func loadHisotry() {
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}
}

func saveHisotry() {
	if f, err := os.Create(historyPath); err != nil {
		fmt.Printf("Error writing history file: %v\n", err)
	} else {
		line.WriteHistory(f)
		f.Close()
	}
}
