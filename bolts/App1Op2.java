package bolts;

public class App1Op2 {
    public static void main(String[] args) {
        execute(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[3]);
    }

    public static void execute(int col1, int col2, String line) {
        String[] lineContents = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        System.out.println(lineContents[col1] + "\n" + lineContents[col2]);
    }
}