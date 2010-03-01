import java.io.BufferedWriter;
import java.io.FileWriter;


public class GenerateTestData {

	public static void main(String[] args) throws Exception {
		BufferedWriter w = new BufferedWriter(new FileWriter("data.txt"));
		for(long i = 0; i < 10000000; i ++)
			w.write(i + "\t" + (long)(Math.random() * 100) + "\t" + (long)(Math.random() * 1000)
					+ "\t" + (long)(Math.random() * 10000) 
					+ "\t" + (Math.random() * 100) + "\t" + (Math.random() * 1000)
					+ "\t" + (Math.random() * 10000) );
		w.close();
	}

}
