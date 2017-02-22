
public class Evaluation {
	private EditDistanceResult editdistanceresult;
	private double maxCount;
	private String classname;
	
	Evaluation(EditDistanceResult editdistanceresult, double maxCount, String classname){
		this.editdistanceresult = editdistanceresult;
		this.maxCount = maxCount;
		this.classname = classname;
	}
	
	public EditDistanceResult getEdit(){
		return editdistanceresult;
	}
	
	public double getMaxCount(){
		return maxCount;
	}
	
	public String getClassName(){
		return classname;
	}
	
	public void setAllElements (EditDistanceResult editdistanceresult, double maxCount, String classname){
		this.editdistanceresult = editdistanceresult;
		this.maxCount = maxCount;
		this.classname = classname;
	}
}
