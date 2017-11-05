package ar.edu.itba.pod.utils;

import org.apache.commons.cli.*;

import static java.lang.System.exit;

public class CLIParser {

    private String[] args;
    private Options options = new Options();

    private static final String helpShortArg = "h";
    private static final String helpArg = "help";

    private static final String addressesArg = "Daddresses";
    private static final String queryNumberArg = "Dquery";
    private static final String inputPathArg = "DinPath";
    private static final String outputPathArg = "DoutPath";
    private static final String timeOutPathArg = "DtimeOutPath";
    private static final String amountArg = "Dn";
    private static final String provinceArg = "Dprov";


    public CLIParser(String[] args) {
        this.args = args;

        options.addOption(helpShortArg, helpArg, false, "show help");
        options.addOption(null, addressesArg, true, "IP Addresses of the nodes");
        options.addOption(null, queryNumberArg, true, "Query to be executed");
        options.addOption(null, inputPathArg, true, "Path to input file");
        options.addOption(null, outputPathArg, true, "Path to output file");
        options.addOption(null, timeOutPathArg, true, "Path to timestamp file");
        options.addOption(null, amountArg, true, "Amount argument (for query 2, 6 or 7)");
        options.addOption(null, provinceArg, true, "Province argument (for query 2)");
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

            if(!cmd.hasOption(queryNumberArg)) throw new IllegalStateException("No query specified, use -Dquery to choose query number. Use -h,--help for more information");
            int queryNumber = Integer.parseInt(cmd.getOptionValue(queryNumberArg));

            if(!cmd.hasOption(inputPathArg)) throw new IllegalStateException("No Input Path specified, use -DinPath to specify path. Use -h,--help for more information");
            String inputPath = cmd.getOptionValue(inputPathArg);

            if(!cmd.hasOption(outputPathArg)) throw new IllegalStateException("No Output Path specified, use -DoutPath to specify path. Use -h,--help for more information");
            String outputPath = cmd.getOptionValue(outputPathArg);

            if(!cmd.hasOption(timeOutPathArg)) throw new IllegalStateException("No TimeStamp Path specified, use -DtimeOutPath to specify path. Use -h,--help for more information");
            String timeOutPath = cmd.getOptionValue(timeOutPathArg);

            arguments = new ConsoleArguments(addresses,queryNumber,inputPath,outputPath,timeOutPath);

            if(cmd.hasOption(amountArg)) {
                arguments.setAmount(Integer.parseInt(cmd.getOptionValue(amountArg)));
            } else {
                if(queryNumber==2 || queryNumber==6 || queryNumber==7) throw new IllegalStateException("There has to be an amount argument for query, add it with -Dn " + queryNumber);

            }

            if(cmd.hasOption(provinceArg)) {
                arguments.setProvince(cmd.getOptionValue(provinceArg));
            } else {
                if(queryNumber==2) throw new IllegalStateException("There has to be a province argument for query, add it with -Dprov " + queryNumber);

            }

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
