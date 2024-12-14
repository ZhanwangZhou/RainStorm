package bolts;

public class App2Op2 {
    public static void main(String[] args) {
        if (args.length == 3 && !args[2].equals("")) {
            execute(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        } else if (args.length == 3) {
            execute(args[0], Integer.parseInt(args[1]), 0);
        }
    }

    public static void execute(String key, int value, int currentCount) {
        System.out.println(key + "\n" + String.valueOf(currentCount + value));
    }
}
