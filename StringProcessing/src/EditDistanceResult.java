
public class EditDistanceResult {
	private double MinimumDistance;
	private double Insertion;
	private double Replacement;
	private double Deletion;
	
	EditDistanceResult(int MinimumDistance, int Insertion, int Replacement, int Deletion){
		this.MinimumDistance = MinimumDistance;
		this.Insertion = Insertion;
		this.Replacement = Replacement;
		this.Deletion = Deletion;
	}
	
	public double getInsertion(){
		return Insertion;
	}
	
	public double getReplacement(){
		return Replacement;
	}
	
	public double getDeletion(){
		return Deletion;
	}
	
	public double getMinimumDistance(){
		return MinimumDistance;
	}
	
	public void IncrementInsertion(){
		Insertion++;
	}
	
	public void IncrementReplacement(){
		Replacement++;
	}
	
	public void IncrementDeletion(){
		Deletion++;
	}
	
	public void setInsertion(double Insertion){
		this.Insertion = Insertion;
	}
	
	public void setDeletion(double Deletion){
		this.Deletion = Deletion;
	}
	
	public void setReplacement(double Replacement){
		this.Replacement = Replacement;
	}
	
	public void setMinimumDistance(double MinimumDistance){
		this.MinimumDistance = MinimumDistance;
	}
	
	public void aggregateEditDistanceElements(EditDistanceResult edr){
		MinimumDistance += edr.MinimumDistance;
		Insertion += edr.Insertion;
		Deletion += edr.Deletion;
		Replacement += edr.Replacement;
	}
}
