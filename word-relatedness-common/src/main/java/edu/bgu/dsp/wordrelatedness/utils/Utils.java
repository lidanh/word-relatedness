package edu.bgu.dsp.wordrelatedness.utils;

import edu.bgu.dsp.wordrelatedness.domain.WordsPair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


public class Utils {
    public static Set<String> stopWords = new HashSet<String>(Arrays.asList("", "a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its", "itself", "i've", "j", "just", "k", "keep	keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure	t", "take", "taken", "taking", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though", "thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want", "wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've", "what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely", "willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www", "x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves", "you've", "z", "zero"));
    public static Set<String> relatedWords = new HashSet<String>(Arrays.asList("tiger,jaguar", "tiger,feline", "closet,clothes", "planet,sun", "hotel,reservation", "planet,constellation", "credit,card", "stock,market", "psychology,psychiatry", "planet,moon", "planet,galaxy", "bank,money", "physics,proton", "vodka,brandy", "war,troops", "Harvard,Yale", "news,report", "psychology,Freud", "money,wealth", "man,woman", "FBI,investigation", "network,hardware", "nature,environment", "seafood,food", "weather,forecast", "championship,tournament", "law,lawyer", "money,dollar", "calculation,computation", "planet,star", "Jerusalem,Israel", "vodka,gin", "money,bank", "computer,software", "murder,manslaughter", "king,queen", "OPEC,oil", "Maradona,football", "mile,kilometer", "seafood,lobster", "furnace,stove", "environment,ecology", "boy,lad", "asylum,madhouse", "street,avenue", "car,automobile", "gem,jewel", "type,kind", "magician,wizard", "football,soccer", "money,currency", "money,cash", "coast,shore", "money,cash", "dollar,buck", "journey,voyage", "midday,noon", "tiger,tiger"));
    public static Set<String> unrelatedWords = new HashSet<String>(Arrays.asList("king,cabbage", "professor,cucumber", "chord,smile", "noon,string", "rooster,voyage", "sugar,approach", "stock,jaguar", "stock,life", "monk,slave", "lad,wizard", "delay,racism", "stock,CD", "drink,ear", "stock,phone", "holy,sex", "production,hike", "precedent,group", "stock,egg", "energy,secretary", "month,hotel", "forest,graveyard", "cup,substance", "possibility,girl", "cemetery,woodland", "glass,magician", "cup,entity", "Wednesday,news", "direction,combination", "reason,hypertension", "sign,recess", "problem,airport", "cup,article", "Arafat,Jackson", "precedent,collection", "volunteer,motto", "listing,proximity", "opera,industry", "drink,mother", "crane,implement", "line,insurance", "announcement,effort", "precedent,cognition", "media,gain", "cup,artifact", "Mars,water", "peace,insurance", "viewer,serial", "president,medal", "prejudice,recognition", "drink,car", "shore,woodland", "coast,forest", "century,nation", "practice,institution", "governor,interview", "money,operation", "delay,news", "morality,importance", "announcement,production", "five,month", "school,center", "experience,music", "seven,series", "report,gain", "music,project", "cup,object", "atmosphere,landscape", "minority,peace", "peace,atmosphere", "morality,marriage", "stock,live", "population,development", "architecture,century", "precedent,information", "situation,isolation", "media,trading", "profit,warning", "chance,credibility", "theater,history", "day,summer", "development,issue"));

    static public boolean deleteDirectory(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    deleteDirectory(files[i]);
                } else {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }


    public static Map calcFMeasure(String filePath) {

        long tp = 0;
        long fp = 0;
        long tn = 0;
        long fn = 0;

        Path results = Paths.get(filePath);
        List<WordsPair> wordsPairs = new ArrayList();

        Charset charset = Charset.forName("ISO-8859-1");
        try {
            List<String> lines = Files.readAllLines(results, charset);

            for (String line : lines) {
                if ( !line.contains("2000")){
                    continue;
                }

                String[] score_yearPairWords = line.split("\t");
                double score = Double.parseDouble(score_yearPairWords[0]);
                String yearPairWords = score_yearPairWords[1];
                String pairWords = yearPairWords.substring(5);
                wordsPairs.add(new WordsPair(pairWords, score));
                wordsPairs.add(new WordsPair(switchWords(pairWords), score));

            }
        } catch (IOException e) {
            System.out.println(e);
        }


        Map<Double, Double> Fs = new HashMap();

        for (double i = 1; i < 11; i += 1) {

            for (WordsPair wp : wordsPairs) {
                if (wp.getScore() > i) {
                    if (relatedWords.contains(wp.getPair())) {
                        // true positive
                        tp++;
                    } else if (unrelatedWords.contains(wp.getPair())) {
                        // false positive
                        fp++;
                    }
                } else {
                    if (relatedWords.contains(wp.getPair())) {
                        // false negative
                        fn++;
                    } else if (unrelatedWords.contains(wp.getPair())) {
                        // true negative
                        tn++;
                    }
                }
            }

            Fs.put(i, calcF(tp,fp,fn,tn));
        }

        return Fs;
    }

    private static String switchWords(String pairWords) {
        String[] words = pairWords.split(",");
        return words[1] + "," + words[0];
    }

    private static double calcF(long tp, long fp, long fn, long tn) {
        if(tp + fp ==0 || tp +fn ==0){
            return 0;
        }

        double precision =  tp / (tp + fp);;
        double recall = tp / (tp +fn);

        if(precision + recall == 0){
            return 0;
        }

        return 2 * ((precision * recall) / (precision + recall));
    }


    public static void scoresToFile(java.util.Map<Double, Double> scores) throws IOException {
        FileWriter fstream;
        BufferedWriter out;

        fstream = new FileWriter("Fmeasures.txt");
        out = new BufferedWriter(fstream);

        Iterator<java.util.Map.Entry<Double, Double>> it = scores.entrySet().iterator();

        while (it.hasNext()) {
            java.util.Map.Entry<Double, Double> pairs = it.next();
            out.write(pairs.getKey() + "\t" + pairs.getValue() + "\n");
        }
        out.close();
    }

    public static List<WordsPair> GetK(String filePath, int k) {
        String currentDecade = "";

        Path results = Paths.get(filePath);

        int count = 0;
        List<WordsPair> wordsPairs = new ArrayList();

        Charset charset = Charset.forName("ISO-8859-1");
        try {
            List<String> lines = Files.readAllLines(results, charset);

            for (String line : lines) {
                String[] score_yearPairWords = line.split("\t");
                String decade = score_yearPairWords[1].substring(0, 4);
                if (!currentDecade.equals(decade)) {
                    currentDecade = decade;
                    count = 0;
                }
                if (count < k) {
                    double score = Double.parseDouble(score_yearPairWords[0]);
                    String yearPairWords = score_yearPairWords[1];
                    wordsPairs.add(new WordsPair(yearPairWords, score));
                    count++;
                }
            }
        } catch (IOException e) {
            System.out.println(e);
        }

        return wordsPairs;
    }

    public static void KsToFile(List<WordsPair> ks) throws IOException {
        FileWriter fstream;
        BufferedWriter out;

        fstream = new FileWriter("Ks.txt");
        out = new BufferedWriter(fstream);

        for(WordsPair wp: ks) {
            out.write(wp.getPair()+ "\t" + wp.getScore() + "\n");
        }
        out.close();
    }
}

