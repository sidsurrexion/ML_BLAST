
public class LocalSequenceAlignment {
	
    private char[] firstStringArray;
    private char[] secondStringArray;
    private int firstStringLength, secondStringLength;
    private double[][] scoreMatrix;
    private int[][] prevCells;
    private double maxScore;
    
    static final int DR_LEFT = 1; 
    static final int DR_UP = 2;   
    static final int DR_DIAG = 4; 
    static final int DR_ZERO = 8; 
    
    public LocalSequenceAlignment(String firstString, String secondString) {
		firstStringLength = firstString.length();
		secondStringLength = secondString.length();
		firstStringArray = firstString.toCharArray();
		secondStringArray = secondString.toCharArray();
		maxScore = Double.MIN_VALUE;
	
		scoreMatrix = new double[firstStringLength+1][secondStringLength+1];
		prevCells = new int[firstStringLength+1][secondStringLength+1];
	
		buildMatrix();
    }
    
    private void buildMatrix() {		
		int i;
	   	int j;
	

		scoreMatrix[0][0] = 0;
		prevCells[0][0] = DR_ZERO;
	

		for (i = 1; i <= firstStringLength; i++) {
			scoreMatrix[i][0] = 0;
		    prevCells[i][0] = DR_ZERO;
		}
	

		for (j = 1; j <= secondStringLength; j++) {
			scoreMatrix[0][j] = 0;
		    prevCells[0][j] = DR_ZERO;
		}

		for (i = 1; i <= firstStringLength; i++) {
		    for (j = 1; j <= secondStringLength; j++) {
			double diagScore = scoreMatrix[i - 1][j - 1] + similarity(i, j);
			double upScore = scoreMatrix[i][j - 1] + similarity(0, j);
			double leftScore = scoreMatrix[i - 1][j] + similarity(i, 0);
	
			scoreMatrix[i][j] = Math.max(diagScore, Math.max(upScore,
				    Math.max(leftScore, 0)));
			if (scoreMatrix[i][j] > maxScore){
				maxScore = scoreMatrix[i][j];
			}
			prevCells[i][j] = 0;
	
			
			if (diagScore == scoreMatrix[i][j]) {
			    prevCells[i][j] |= DR_DIAG;
			}
			if (leftScore == scoreMatrix[i][j]) {
			    prevCells[i][j] |= DR_LEFT;
			}
			if (upScore == scoreMatrix[i][j]) {
			    prevCells[i][j] |= DR_UP;
			}
			if (0 == scoreMatrix[i][j]) {
			    prevCells[i][j] |= DR_ZERO;
			}
		    }
		}
    }
    
    private int characterSimilarityScore(char firstChar, char secondChar){
    	if (firstChar == secondChar){
    		return 1;
    	}
    	return -1;
    }
    
    private double similarity(int i, int j) {
		return characterSimilarityScore(firstStringArray[i - 1], secondStringArray[j-1]); 
    }
    
    public double getMaxScore(){
    	return maxScore;
    }
    
    private void printAlignments(int i, int j, String aligned1, String aligned2) {	
    	
    	if ((prevCells[i][j] & DR_ZERO) > 0) {
		    System.out.println(aligned1);
		    System.out.println(aligned2);
		    System.out.println("");
		    return;
		}
	
		// find out which directions to backtrack
		if ((prevCells[i][j] & DR_LEFT) > 0) {
		    printAlignments(i-1, j, firstStringArray[i-1] + aligned1, "_" + aligned2);
		}
		if ((prevCells[i][j] & DR_UP) > 0) {
		    printAlignments(i, j-1, "_" + aligned1, secondStringArray[j-1] + aligned2);
		}
		if ((prevCells[i][j] & DR_DIAG) > 0) {
		    printAlignments(i-1, j-1, firstStringArray[i-1] + aligned1, secondStringArray[j-1] + aligned2);
		}
    }
}
