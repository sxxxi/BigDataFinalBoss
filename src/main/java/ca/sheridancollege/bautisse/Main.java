package ca.sheridancollege.bautisse;

import ca.sheridancollege.bautisse.drivers.AverageUsage;
import ca.sheridancollege.bautisse.drivers.FileImport;
import ca.sheridancollege.bautisse.drivers.MaxEnergy;

public class Main {
    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "max": {
                MaxEnergy.execute(args[1], args[2], args[3], args[4]);
            }
            case "import": {
                FileImport.execute(args[1], args[2]);
            }
            case "ave": {
                AverageUsage.execute(args[1], args[2], args[3], args[4]);
            }
        }
    }
}