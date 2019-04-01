
public class Main {

	public static void main(String[] args) {
		
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		int result = 3+5;
		System.out.println(result);

	}

}
