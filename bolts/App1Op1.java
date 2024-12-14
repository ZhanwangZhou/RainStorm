package bolts;

public class App1Op1 {
    public static void main(String[] args) {
        execute(Integer.parseInt(args[0]), args[1], args[3]);
    }

    public static void execute(int col, String pattern, String line) {
        String[] lineContents = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        if(pattern.equals(lineContents[col])) {
            System.out.println(lineContents[2] + "\n" + line);
        }
    }
}
