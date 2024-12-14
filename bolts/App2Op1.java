package bolts;

public class App2Op1 {
    public static void main(String[] args) {
        execute(args[0], args[2]);
    }

    public static void execute(String word, String line) {
        String[] lineContents = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if (word.equals(lineContents[6])) {  // col6 = "sign post"
            System.out.println(lineContents[8] + "\n1");  // key = category
        }
    }
}
