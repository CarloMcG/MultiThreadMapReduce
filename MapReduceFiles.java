import java.util.*;
import java.util.ArrayList;
import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;


public class MapReduceFiles {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt {Number of lines per mapping thread} {Number of groups per reduction thread}");
        }

        Map<String, String> input = new HashMap<String, String>();
        try {
            for (int fileCounter = 0; fileCounter < 3; fileCounter++){ // Add all 3 files to a single hashmap to help with part 4 of this question
                input.put(args[0], readFile(args[0]));
                input.put(args[1], readFile(args[1]));
                input.put(args[2], readFile(args[2]));
            }
        }
        catch (IOException ex)
        {
            System.err.println("Error reading files...\n" + ex.getMessage());
            ex.printStackTrace();
            System.exit(0);
        }
    int numLines = Integer.parseInt(args[3]);
    int numGroups = Integer.parseInt(args[4]);



        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            // Get time
            long mapStart = System.currentTimeMillis();
            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            while(inputIter.hasNext()) {
                String[] files = new String[numLines]; // Array to story entry key
                String[] conts = new String[numLines]; // Array to store entry value
                for(int i = 0; i < numLines; i++) { // Only iterates the desired number of lines at a time
                    while(inputIter.hasNext()) {
                        Map.Entry<String, String> entry = inputIter.next();
                        files[i] = entry.getKey();
                        conts[i] = entry.getValue();
                    }
                }
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for(int i = 0; i < numLines; i++) {
                            map(files[i], conts[i], mapCallback); // runs the map function with the data stored in the arrays above
                        }
                    }
                });
                mapCluster.add(t);
                t.start();
            }



            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // Get time
            long mapEnd = System.currentTimeMillis();
            long groupStart = System.currentTimeMillis();

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word.replaceAll("[^a-zA-Z]", ""), list); // Also removes non alphabetic characters
                }
                list.add(file);
            }
            // Get time
            long groupEnd = System.currentTimeMillis();
            long reduceStart = System.currentTimeMillis();

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };



            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());


            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                String[] words = new String[numGroups]; // Array to story entry key
                String[][] listArray = new String[numGroups][]; // Array of arrays to store value
                for(int i = 0; i < numGroups; i++) {
                    while(groupedIter.hasNext()) {
                        Map.Entry<String, List<String>> entry = groupedIter.next();
                        words[i] = entry.getKey();
                        final List<String> list = entry.getValue();
                        for (int j = 0; j < list.size(); j++) {
                            listArray[i][j] = list.get(j); // convert to array
                        }
                    }
                }

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for(int i = 0; i < numGroups; i++) {
                            List<String> list = Arrays.asList(listArray[i]); // back to a string to work with reduce()
                            reduce(words[i], list, reduceCallback);
                        }
                    }
                });
                reduceCluster.add(t);
                t.start();
            }



            // Get time
            long reduceEnd = System.currentTimeMillis();
            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            System.out.println(output);

            long mapTime = mapEnd - mapStart;
            long groupTime = groupEnd - groupStart;
            long reduceTime = reduceEnd - reduceStart;

            System.out.println("\n\n Mapping Time: " + mapTime + " ms \t Grouping Time: " + groupTime + " ms \t Reduction Time: " + reduceTime +" ms"); // print time

        }
    }
    // Map Function

    public static void map(String file, String contents, List<MappedItem> mappedItems) {
        String[] words = contents.trim().split("\\s+");
        for(String word: words) {
            mappedItems.add(new MappedItem(word, file));
        }
    }
    // Reduce Function
    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        output.put(word, reducedList);
    }

    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine());
            }
            while (scanner.hasNextLine()) {
                fileContents.append(lineSeparator + scanner.nextLine());
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

}