import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class Answer2 {
    private static final int THREAD_COUNT = 3;
    private static final String FILE_NAME = "simpsons_script_lines.csv";
    private static String episode = "";
    private static String location = "";
    private static String bartWord = "";
    private static String lisaWord = "";
    private static String homerWord = "";
    private static String margeWord = "";
    private static int counter;
    private static int lineCount;
    private static int bartCount;
    private static int lisaCount;
    private static int homerCount;
    private static int margeCount;

    public static void main(String[] args) {
        List<String> lines = loadDataFromFile();
        // The following is an alternative in case you want to load the file directly from ECDC.
        System.out.println("Loaded " + lines.size() + " lines");
        String headers = lines.remove(0); //remove headers

        for (int co = 0; co < THREAD_COUNT + 1; co++) {
            // Create starting timestamp
            Date begin = new Date();
            String num = new DecimalFormat("#").format((int) Math.pow(2, co));
            String nOfThreads = "";
            String underLine = "";
            // Initiating mandatory variables
            counter = 0;
            lineCount = 0;
            bartCount = 0;
            lisaCount = 0;
            homerCount = 0;
            margeCount = 0;

            // Handle the case that no data is loaded from the file
            if (lines.isEmpty()) {
                System.err.println("No data...");
                System.exit(1);
            }

            if (co == 0) {
                nOfThreads = "Thread";
                underLine = "----------------------";
            } else {
                nOfThreads = "Threads";
                underLine = "-----------------------";
            }
            System.out.print("\nExecute with " + num + " " + nOfThreads + ".");
            System.out.println("\n" + underLine);
            ProcessThread[] threads = new ProcessThread[(int) Math.pow(2, co)];
            // In this case we may have division with remainder, therefore we should address this later on.
            int batchSize = lines.size() / (int) Math.pow(2, co);
            for (int i = 0; i < threads.length; i++) {
                int start = i * batchSize;
                int end = (i + 1) * batchSize;

                // Check whether the last batch should be extended to process the lines left
                // (case of non perfect division).
                if (i == threads.length - 1 && end < lines.size()) {
                    System.out.println("End is extended from " + end + " to " + lines.size());
                    end = lines.size();
                }

                // Pass the appropriate sublist to the thread for processing.
                threads[i] = new ProcessThread(lines.subList(start, end), i);
                threads[i].start();
            }

            for (ProcessThread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Initiating mandatory HashMaps.
            HashMap<String, Integer> mostWordCountEpisode = new HashMap<>();
            HashMap<String, Integer> mostLinesEpisode = new HashMap<>();
            HashMap<String, Integer> mostWordsForBart = new HashMap<>();
            HashMap<String, Integer> mostWordsForLisa = new HashMap<>();
            HashMap<String, Integer> mostWordsForHomer = new HashMap<>();
            HashMap<String, Integer> mostWordsForMarge = new HashMap<>();

            // Merge the results from the different threads
            for (ProcessThread thread : threads) {
                // Merge wordsPerEpisodes
                thread.getWordsPerEpisodes().forEach((episodeID, words) -> {
                    int wordsSum = words;
                    if (mostWordCountEpisode.containsKey(episodeID)) {
                        wordsSum += mostWordCountEpisode.get(episodeID);
                    }
                    mostWordCountEpisode.put(episodeID, wordsSum);
                });

                // Merge mostLinesEpisode
                thread.getMostLinesEpisode().forEach((rawLocationText, texts) -> {
                    int linesSum = texts;
                    if (mostLinesEpisode.containsKey(rawLocationText)) {
                        linesSum += mostLinesEpisode.get(rawLocationText);
                    }
                    mostLinesEpisode.put(rawLocationText, linesSum);
                });

                // Merge mostBartWords
                thread.getMostWordsForBart().forEach((bartWords, bartWordsCounter) -> {
                    int bartSum = bartWordsCounter;
                    if (mostWordsForBart.containsKey(bartWords)) {
                        bartSum += mostWordsForBart.get(bartWords);
                    }
                    mostWordsForBart.put(bartWords, bartSum);
                });

                // Merge mostLisaWords
                thread.getMostWordsForLisa().forEach((lisaWords, lisaWordsCounter) -> {
                    int lisaSum = lisaWordsCounter;
                    if (mostWordsForLisa.containsKey(lisaWords)) {
                        lisaSum += mostWordsForLisa.get(lisaWords);
                    }
                    mostWordsForLisa.put(lisaWords, lisaSum);
                });

                // Merge mostHomerWords
                thread.getMostWordsForHomer().forEach((homerWords, homerWordsCounter) -> {
                    int homerSum = homerWordsCounter;
                    if (mostWordsForHomer.containsKey(homerWords)) {
                        homerSum += mostWordsForHomer.get(homerWords);
                    }
                    mostWordsForHomer.put(homerWords, homerSum);
                });

                // Merge mostMargeWords
                thread.getMostWordsForMarge().forEach((margeWords, margeWordsCounter) -> {
                    int margeSum = margeWordsCounter;
                    if (mostWordsForMarge.containsKey(margeWords)) {
                        margeSum += mostWordsForMarge.get(margeWords);
                    }
                    mostWordsForMarge.put(margeWords, margeSum);
                });
            }

            // Calculating-Searching for final data to display
            // For the episode that contains the most words
            mostWordCountEpisode.forEach((episodeID, words) -> {
                if (mostWordCountEpisode.get(episodeID) > counter) {
                    counter = words;
                    episode = episodeID;
                }
            });

            // For the location that contains the most lines
            mostLinesEpisode.forEach((rawLocationText, texts) -> {
                if (texts > lineCount) {
                    lineCount = texts;
                    location = rawLocationText;
                }
            });

            // For the word said most times from Bart
            mostWordsForBart.forEach((bartWords, bartWordsCounter) -> {
                if (bartWordsCounter > bartCount) {
                    bartCount = bartWordsCounter;
                    bartWord = bartWords;
                }
            });

            // For the word said most times from Lisa
            mostWordsForLisa.forEach((lisaWords, lisaWordsCounter) -> {
                if (lisaWordsCounter > lisaCount) {
                    lisaCount = lisaWordsCounter;
                    lisaWord = lisaWords;
                }
            });

            // For the word said most times from Homer
            mostWordsForHomer.forEach((homerWords, homerWordsCounter) -> {
                if (homerWordsCounter > homerCount) {
                    homerCount = homerWordsCounter;
                    homerWord = homerWords;
                }
            });

            // For the word said most times from Marge
            mostWordsForMarge.forEach((margeWords, margeWordsCounter) -> {
                if (margeWordsCounter > margeCount) {
                    margeCount = margeWordsCounter;
                    margeWord = margeWords;
                }
            });

            // Create ending timestamp
            Date finish = new Date();
            System.out.println("Time taken in milli seconds: " + (finish.getTime() - begin.getTime()));
        }
        // Displaying data
        System.out.println("----------- RESULTS ------------");
        System.out.println("Most Words Episode: " + episode);
        System.out.println("Most Lines Episode: " + location);
        System.out.println("Bart Used Word: " + bartWord + ", " + bartCount + " times.");
        System.out.println("Lisa Used Word: " + lisaWord + ", " + lisaCount + " times.");
        System.out.println("Homer Used Word: " + homerWord + ", " + homerCount + " times.");
        System.out.println("Marge Used Word: " + margeWord + ", " + margeCount + " times.");
    }

    // This method loads the data from a file
    static List<String> loadDataFromFile() {
        System.out.println("Loading " + FILE_NAME);
        List<String> lines = new ArrayList<>();
        try {
            BufferedReader in = new BufferedReader(new FileReader(FILE_NAME));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                lines.add(inputLine);
            }
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }

    // This thread processes the file lines.
    static class ProcessThread extends Thread {
        private final List<String> lines;
        private final HashMap<String, Integer> mostWordCountEpisode = new HashMap<>();
        private final HashMap<String, Integer> mostLinesEpisode = new HashMap<>();
        private final HashMap<String, Integer> mostWordsForBart = new HashMap<>();
        private final HashMap<String, Integer> mostWordsForLisa = new HashMap<>();
        private final HashMap<String, Integer> mostWordsForHomer = new HashMap<>();
        private final HashMap<String, Integer> mostWordsForMarge = new HashMap<>();

        public ProcessThread(List<String> lines, int i) {
            this.lines = lines;
        }

        @Override
        public void run() {
            for (String line : lines) {
                String[] columns = line.split(",");

                // skip lines with errors
                if (columns.length != 9) {
                    continue;
                }
                // Initiating variables for needed data
                String episodeID = columns[1];
                String characterID = columns[3];
                String rawLocationText = columns[6];
                int words;
                int texts = 0;

                // skip corrupted lines
                try {
                    words = countWordsUsingSplit(columns[7]);
                    if (words != 0) {
                        texts = 1;
                    }
                } catch (NumberFormatException e) {
                    continue;
                }

                processWords(episodeID, words);
                processLocation(rawLocationText, texts);

                switch (characterID) {
                    case "8":
                        processBart(columns[7]);
                        break;
                    case "9":
                        processLisa(columns[7]);
                        break;
                    case "2":
                        processHomer(columns[7]);
                        break;
                    case "1":
                        processMarge(columns[7]);
                        break;
                }
            }
        }

        public HashMap<String, Integer> getWordsPerEpisodes() {
            return mostWordCountEpisode;
        }

        public HashMap<String, Integer> getMostLinesEpisode() {
            return mostLinesEpisode;
        }

        public HashMap<String, Integer> getMostWordsForBart() {
            return mostWordsForBart;
        }

        public HashMap<String, Integer> getMostWordsForLisa() {
            return mostWordsForLisa;
        }

        public HashMap<String, Integer> getMostWordsForHomer() {
            return mostWordsForHomer;
        }

        public HashMap<String, Integer> getMostWordsForMarge() {
            return mostWordsForMarge;
        }

        // This methods is used for the episode with the most words
        private void processWords(String episodeID, int words) {
            int wordsSum = words;
            if (mostWordCountEpisode.containsKey(episodeID)) {
                wordsSum += mostWordCountEpisode.get(episodeID);
            }
            mostWordCountEpisode.put(episodeID, wordsSum);
        }

        // This methods is used for the location with the most lines
        private void processLocation(String rawLocationText, int texts) {
            int lineCounter = texts;
            if (mostLinesEpisode.containsKey(rawLocationText)) {
                lineCounter += mostLinesEpisode.get(rawLocationText);
            }
            mostLinesEpisode.put(rawLocationText, lineCounter);
        }

        // This methods is used for the word, Bart uses most
        private void processBart(String wordsBart) {
            int bartWords;
            String[] arrSplit = wordsBart.split(" ");
            for (int i = 0; i < arrSplit.length; i++) {
                if (arrSplit[i].length() > 4) {
                    arrSplit[i] = arrSplit[i].toLowerCase();
                    if (mostWordsForBart.containsKey(arrSplit[i])) {
                        bartWords = mostWordsForBart.get(arrSplit[i]) + 1;
                        mostWordsForBart.put(arrSplit[i], bartWords);
                    } else {
                        mostWordsForBart.put(arrSplit[i], 1);
                    }
                }
            }
        }

        // This methods is used for the word, Lisa uses most
        private void processLisa(String wordsLisa) {
            int lisaWords;
            String[] arrSplit = wordsLisa.split(" ");
            for (int i = 0; i < arrSplit.length; i++) {
                if (arrSplit[i].length() > 4) {
                    arrSplit[i] = arrSplit[i].toLowerCase();
                    if (mostWordsForLisa.containsKey(arrSplit[i])) {
                        lisaWords = mostWordsForLisa.get(arrSplit[i]) + 1;
                        mostWordsForLisa.put(arrSplit[i], lisaWords);
                    } else {
                        mostWordsForLisa.put(arrSplit[i], 1);
                    }
                }
            }
        }

        // This methods is used for the word, Homer uses most
        private void processHomer(String wordsHomer) {
            int homerWords;
            String[] arrSplit = wordsHomer.split(" ");
            for (int i = 0; i < arrSplit.length; i++) {
                if (arrSplit[i].length() > 4) {
                    arrSplit[i] = arrSplit[i].toLowerCase();
                    if (mostWordsForHomer.containsKey(arrSplit[i])) {
                        homerWords = mostWordsForHomer.get(arrSplit[i]) + 1;
                        mostWordsForHomer.put(arrSplit[i], homerWords);
                    } else {
                        mostWordsForHomer.put(arrSplit[i], 1);
                    }
                }
            }
        }

        // This methods is used for the word, Marge uses most
        private void processMarge(String wordsMarge) {
            int margeWords;
            String[] arrSplit = wordsMarge.split(" ");
            for (int i = 0; i < arrSplit.length; i++) {
                if (arrSplit[i].length() > 4) {
                    arrSplit[i] = arrSplit[i].toLowerCase();
                    if (mostWordsForMarge.containsKey(arrSplit[i])) {
                        margeWords = mostWordsForMarge.get(arrSplit[i]) + 1;
                        mostWordsForMarge.put(arrSplit[i], margeWords);
                    } else {
                        mostWordsForMarge.put(arrSplit[i], 1);
                    }
                }
            }
        }
    }

    // This method counts the number of words in a String
    public static int countWordsUsingSplit(String input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }
        String[] words = input.split("\\s+");
        return words.length;
    }
}
