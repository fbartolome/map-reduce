package ar.edu.itba.pod.utils;

import org.apache.commons.cli.*;

import static java.lang.System.exit;

public class CLIParser {

    private String[] args;
    private Options options = new Options();

    private static final String helpShortArg = "h";
    private static final String helpArg = "help";

    private static final String addressesArg = "Daddresses";


    public CLIParser(String[] args) {
        this.args = args;

        options.addOption(helpShortArg, helpArg, false, "show help");
        options.addOption(null, addressesArg, true, "IP Addresses of the nodes");
    }

    public ConsoleArguments parse() {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        ConsoleArguments arguments = null;
        try {
            cmd = parser.parse(options, args);

            if(cmd.hasOption(helpShortArg)) {
                showHelp();
                exit(0);
            }

            if(!cmd.hasOption(addressesArg)) throw new IllegalStateException("No IP addresses specified, use -Daddresses to specify addresses. Use -h,--help for more information");
            String[] addresses = cmd.getOptionValue(addressesArg).split(";");

            arguments = new ConsoleArguments(addresses);

        } catch (Exception e) {
            System.err.println("An error was found: " + e.getMessage());
            exit(-1);
        }

        return arguments;
    }

    private void showHelp() {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("Main", options);
    }
}
