package com.opstty;

import com.opstty.job.HighestTree;
import com.opstty.job.NumberSpecies;
import com.opstty.job.Species;
import com.opstty.job.District;
import com.opstty.job.WordCount;
import com.opstty.job.HighTree;
import com.opstty.job.OldestTreeDistrict;
import com.opstty.job.MostTreeDistrict;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("district", District.class,
                    "A MapReduce job that displays the list of district containing trees in this files");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that display the trees species in the input files.");

            programDriver.addClass("nspecies", NumberSpecies.class,
                    "A map/reduce program that display the trees species and the number in the input files.");

            programDriver.addClass("highest", HighestTree.class,
                    "A map/reduce program that display the trees species and the height of the higher tree in the input files.");

            programDriver.addClass("mostTreeD", MostTreeDistrict.class,
                    "A map/reduce program that display the district with the most trees in the input files.");

            programDriver.addClass("highestTrees", HighTree.class,
                    "A map/reduce program that display the trees sorted by height in the input files.");

            programDriver.addClass("oldestTreeD", OldestTreeDistrict.class,
                    "A map/reduce program that display the oldest tree and his district in the input files.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
