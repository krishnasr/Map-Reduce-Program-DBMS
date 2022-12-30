import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// This is a mapper class for actor input file

class ActorsMapper extends Mapper<Object, Text, Text, Text>{    
    
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException{    
    	
    	Text titleIdTxt = new Text();
    	Text actorValue = new Text();
    	String line = value.toString();    
        
    	//splitting file line on semicolon delimiter
    	String data[] = line.split(",");
        String titleId = data[0].trim();
        titleIdTxt.set(titleId);
        String actorId = data[1].trim();	
        String actorName = data[2].trim();
        
        StringBuffer actorData = new StringBuffer();
        
        //prefixing actor data with "actorData::" to identify it in reducer
        actorData.append("actorData::").append(actorId).append(";").append(actorName);
        actorValue.set(actorData.toString());
        
        //exporting actor data from mapper with titleId as key and actor data (id;name) as a value.
        context.write(titleIdTxt, actorValue);
    }
}

// This is a mapper class for Crews input file

class CrewMapper extends Mapper<Object, Text, Text, Text>{    
    
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException{    
    	
    	Text titleIdTxt = new Text();
    	Text directorValue = new Text();
    	String line = value.toString();    
        
    	//splitting file line on semicolon delimiter
    	String data[] = line.split("\t");
        String titleId = data[0].trim();
        titleIdTxt.set(titleId);
        
        //prefixing director data with "directorId::" to identify it in reducer
        String directorId = "directorId::"+data[1].trim();
        directorValue.set(directorId);
        
        //exporting director data from mapper with titleId as key and directorId as a value.
        context.write(titleIdTxt, directorValue);
    }
}

// This is a mapper class for Titles input file

class TitlesMapper extends Mapper<Object, Text, Text, Text>{    

 public void map(Object key, Text value, Context context) throws IOException, InterruptedException{   

		Text titleIdTxt = new Text();
		Text titleValue = new Text();

		//titileType that we need to consider
		String titleTypeToConsider = "tvEpisode";
		
		String line = value.toString();

		//split file line (each record) on semicolon delimiter
		String data[] = line.split("\t");

		String titleId = data[0].trim();
		titleIdTxt.set(titleId);

		//populating titleType only if its from one of the titleTypes that we are searching for
		String titleType = titleTypeToConsider.equals(data[1].trim()) ? data[1].trim() : null;

		String titleName = data[2].trim();
		
		//if year has \N in it, replacing that with 0
		Integer year = data[5].trim().equals("\\N") || data[5].trim().equals("startYear") ? 0 : Integer.parseInt(data[5].trim());
		year = 1997 <= year && year <= 2007 ? year : 0;

		//removing all occurrences of \N with or without comma from genres
		String genre = data[8].replace("\\N,", "").replace(",\\N", "").replace("\\N","").trim();
		genre = null == genre || "".equals(genre) ? "NO_GENRE_SPECIFIED" : genre;
		
		StringBuffer titleData = new StringBuffer();

		/*
		 * Exporting title data only and only if it matches titletype tvEpisode and if
		 * startyear is in between 1997 to 2007. Hence the null check condition.
		 * This will make reducer lightweight as we will not go through all the titles there.
		 * Prefixing title data with "titleData::" to identify it in reducer
		 * Titles data will be exported from mapper with titleId as key and title 
		 * data (titleType;titleName;year;genre) as a value.
		 */

		if(null != titleType && year != 0) {
			titleData.append("titleData::").append(titleType).append(";").append(titleName).append(";")
			.append(year).append(";").append(genre);
			titleValue.set(titleData.toString());
			context.write(titleIdTxt, new Text(titleData.toString()));
		}
	}
}

//This is a reducer class for 3 mappers of Titles, Actors and Directors/crew

class ImdbReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	@Override
	public void reduce(Text titleId, Iterable<Text> mappedData, Context context) 
			 throws IOException, InterruptedException {

		String titleData = null;
		Map<String,String> actorIdNameMap = new HashMap<String,String>();	//[actorId,actorName]
		List<String> directors = new ArrayList<String>();
		List<String> actorDirectorNames = new ArrayList<String>();
		
		//looping over all entries received for the key of titleId from 3 mappers 
		for (Text txtData : mappedData) {
			String data = txtData.toString();
			
			/*
			 * Checking if the data received is from TitlesMapper or ActorsMapper or
			 * CrewMapper based on the value prefix
			 */ 
			if(data.startsWith("titleData::")) {
				titleData = data.split("titleData::")[1];
				
			}else if(data.startsWith("actorData::")) {
				String []actorData = data.split("actorData::");
				actorData = actorData[1].split(";");
				//pushing actors information in a map of actorId and actorName
				actorIdNameMap.put(actorData[0],actorData[1]);
			}else if(data.startsWith("directorId::")){
				//There can be multiple directors for the title, hence the list.
				directors.addAll(Arrays.asList(data.split("::")[1].split(",")));
			}
		}
		
		/*
		 * There can be multiple directors for the title. 
		 * For every directorId received, checking if that Id is also present in the actors list for that title.
		 * If found, that means the director is also acting in the title. 
		 * Pushing those director/actor names in the actorDirectorNames list as there can more than one for the title.
		 */
		for(String directorId : directors) {
			if(null != actorIdNameMap.get(directorId)) {
				actorDirectorNames.add(actorIdNameMap.get(directorId));
			}
		}
		
		
		/*
		 * Writing data to output file only if we find titles where actor is also
		 * a director. Hence the null check.
		 */
		if(null!= titleData && actorDirectorNames.size() > 0) {
			String [] titleDataArr = titleData.split(";");
			String type = titleDataArr[0];
			String title = titleDataArr[1];
			String year = titleDataArr[2];
			String genre = titleDataArr[3];
			
			
			 // If there are more than one directors found acting in title then printing separate row for each in the output file
			 
			for(String name : actorDirectorNames) {
				StringBuffer output = new StringBuffer();
				output.append(title).append("\t").append(name).append("\t")
				.append(genre).append("\t").append(year);
			
				context.write(NullWritable.get(), new Text(output.toString()));
			}
		}
	}
}

// This is main runner class of Hadoop Map/Reduce program for IMDB database

public class MainRunner extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		//Configuration Object
		Configuration conf = getConf();
		//int split = 700*1024*1024;
		int split = 400*1024*1024;
		String splitsize = Integer.toString(split);
		conf.set("mapred.min.split.size", splitsize);
	   	conf.set("mapreduce.map.memory.mb", "2048");	 
	  	conf.set("mapreduce.reduce.memory.mb", "2048");
	  	
	  	
		//Printing argument list input/output files path
		for(int i=0; i<args.length; i++) {
			System.out.println("Argument "+i+" :: "+args[i]);
		}
		

		Job job = Job.getInstance(conf, "IMDB");
		job.setJarByClass(MainRunner.class);
		//job.setNumReduceTasks(1);	//With 1 reducer
		job.setNumReduceTasks(2);	//With 2 reducer
		job.setReducerClass(ImdbReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//3 Input files with their mappers for Titles, Actors and Crew/Directors respectively.
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TitlesMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ActorsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CrewMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		boolean result = job.waitForCompletion(true);
		return (result) ? 0 : 1;
	}   
	
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new MainRunner(), args);
		long endTime = System.currentTimeMillis();
		long duration = (endTime - startTime);
		System.out.println("Total time taken in miliseconds for execution :: "+duration);
		System.out.println("Total time taken in seconds for execution :: "+duration/60);
		System.exit(exitCode);

	}

}