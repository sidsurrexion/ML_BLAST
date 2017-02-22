import java.util.Scanner;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

public class Visualization extends ApplicationFrame {

    public Visualization(String applicationTitle , String chartTitle, String[] all_years, String[] all_counts,
                         String horizontal_text,
                         String vertical_text) {
        super(applicationTitle);
        // TODO Auto-generated constructor stub
        JFreeChart barChart = ChartFactory.createBarChart( chartTitle, horizontal_text, vertical_text,
                createDataset(all_years, all_counts), PlotOrientation.VERTICAL, true, true, false);

        ChartPanel chartPanel = new ChartPanel( barChart );

        chartPanel.setPreferredSize(new java.awt.Dimension( 400 , 400 ) );

        setContentPane( chartPanel );
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        System.out.println("Please enter the specific mode: ");
        Scanner scanner = new Scanner(System.in);
        String string_choice = scanner.nextLine();
        String file_path_whole = "<ConfusionVector.txt>";
        String file_path_individual = "<ClassOperation.txt>";

        File file;

        if (string_choice.equals("a")){
            file = new File(file_path_whole);
        } else {
            file = new File(file_path_individual);
        }

        try{
            BufferedReader buffer = new BufferedReader(new FileReader(file));
            if(string_choice.equals("a")){
                file_operation_modeWhole(buffer);
            } else {
                file_operation_modeIndividual(buffer);
            }
            buffer.close();
            scanner.close();
        } catch  (Exception e){
            e.printStackTrace();
        }

    }

    private static void file_operation_modeWhole(BufferedReader buffer) throws IOException{
        String line;
        String[] classes = new String[9];
        String[] numbers = new String[9];

        int i = 0;
        while ((line = buffer.readLine()) != null) {
            String[] split_data = line.split(":");
            classes[i] = split_data[0];
            numbers[i] = split_data[1];
            i++;
        }

        Visualization visualization = new Visualization("Confusion Histogram ",
                "Accuracy Histogram for Classes", classes, numbers,
                "Classes", "Classification Percentage");
        visualization.pack( );
        RefineryUtilities.centerFrameOnScreen(visualization);
        visualization.setVisible(true);
    }

    private static void file_operation_modeIndividual(BufferedReader buffer) throws IOException{
        String line;
        int i;
        while ((line = buffer.readLine()) != null) {
            String[] classOperations = line.split(",");
            String className = classOperations[0].split(":")[1];
            String[] classOperationsForDisplay = new String[4];
            String[] classOperationsForResult = new String[4];
            for (i =  1; i <= 4; i++){
               String[] cc = classOperations[i].split(",");
                classOperationsForDisplay[i-1] = cc[0];
                classOperationsForResult[i-1] = cc[1];
            }
            String fraction = classOperations[5].split(":")[1];
            String process = classOperations[6].split(":")[1];

            String chartTitle = process + " for fraction " + fraction + " and for class " + className;
            Visualization visualization = new Visualization("Class Operations",
                    "Classes",
                    classOperationsForDisplay, classOperationsForResult,
                    "Class Operations", "Index Value");
            visualization.pack( );
            RefineryUtilities.centerFrameOnScreen(visualization);
            visualization.setVisible(true);
        }
    }


    // Creating X and Y axis values
    private CategoryDataset createDataset( String[] all_years, String[] all_counts )
    {
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset( );
        for(int i = 0; i < all_years.length; i++){
            dataset.addValue(Double.parseDouble(all_counts[i]), all_years[i], "Comparison");
        }
        return dataset;
    }

}