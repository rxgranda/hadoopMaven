import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.GenericOptionsParser;

 class TaxiIDDatePair implements Writable, WritableComparable<TaxiIDDatePair> {

    private IntWritable taxiID = new IntWritable();                 // natural key
    private LongWritable startDateMillis = new LongWritable();// secondary key
    //private Text info = new Text();// parsedInfo

    public TaxiIDDatePair(){}

    public TaxiIDDatePair(int oTaxiID,long oStartDataMillis){
        taxiID.set(oTaxiID);
        startDateMillis.set(oStartDataMillis);
        //info.set(oInfo);
    }

    public IntWritable getTaxiID() {
        return taxiID;
    }

    public void setTaxiID(IntWritable otaxiID) {
        taxiID = otaxiID;
    }

    public LongWritable getStartDateMillis() {
        return startDateMillis;
    }

    public void setStartDateMillis(LongWritable ostartDateMillis) {
        startDateMillis = ostartDateMillis;
    }

//        public Text getInfo() {
//            return info;
//        }
//
//        public void setInfo(Text info) {
//            this.info = info;
//        }




    @Override
    /**
     * This comparator controls the sort order of the keys.
     */
    public int compareTo(TaxiIDDatePair pair) {
        int compareValue = taxiID.compareTo(pair.getTaxiID());
        if (compareValue == 0) {
            compareValue = startDateMillis.compareTo(pair.getStartDateMillis());
        }
        return compareValue;    // sort ascending
        //return -1*compareValue;   // sort descending
    }
    @Override
    public void write(DataOutput out) throws IOException {
        taxiID.write(out);
        startDateMillis.write(out);
        //info.write(out);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        taxiID.readFields(in);
        startDateMillis.readFields(in);
        //info.readFields(in);
    }

    @Override
    public int hashCode() {
        return taxiID.hashCode();
    }

     @Override
     public boolean equals(Object o)
     {
         if(o instanceof TaxiIDDatePair)
         {
             TaxiIDDatePair tp = (TaxiIDDatePair) o;
             return taxiID.equals(tp.getTaxiID()) && startDateMillis.equals(tp.startDateMillis);
         }
         return false;
     }
}


public class TaxiRecords {




    /* --- subsection 1.1 and 1.2 ------------------------------------------ */
    public static class TaxiDriverMapper
            extends Mapper<LongWritable, Text, TaxiIDDatePair, Text> {
        private static final int MISSING = 9999;
        private static final String EMPTY_STATUS = "'E'";
        private static final String CLIENT_STATUS = "'M'";

        static final float radius=6371.009f;
        static final float toRadians= (float)(Math.PI / 180);
        static final float top = 49.3457868f ;// north lat
        static final float left = -124.7844079f;// west long
        static final float right = -66.9513812f; // east long
        static final float bottom =  24.7433195f; // south lat

        static final ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");





        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] info = value.toString().split(",");
            //706,'2010-02-28 23:46:08',37.66721,-122.41029,'E','2010-03-01 04:02:28',37.6634,-122.43123,'E'
            System.out.println(info.length);
            if (info.length>=9) {
                if(info.length>9)
                    System.out.println("[Exceeded] "+value.toString());
                String statusStart = info[1];
                statusStart=statusStart.substring(1,2);
                String statusEnd = info[info.length - 1];
                statusEnd=statusEnd.substring(1,2);
                if(statusStart.equals(EMPTY_STATUS)&& statusEnd.equals(EMPTY_STATUS)){
                    System.out.println("[Skip empty track]");
                }

                int id = Integer.parseInt(info[0]);
                String dateStart = info[1];
                float starLat = Float.parseFloat(info[2]);
                float starLong = Float.parseFloat(info[3]);

                String dateEnd = info[info.length - 4];
                float endLat = Float.parseFloat(info[info.length - 3]);
                float endLong =Float.parseFloat( info[info.length - 2]);




                if  ((bottom<starLat && starLat<top )&& (bottom<endLat&&endLat<top )&&(left<starLong&&starLong<right)&&(left<endLong&&endLong<right)){
                    double deltaLat = Math.pow((starLat - endLat) * toRadians, 2f);
                    double deltaLong = (starLong - endLong) * toRadians;
                    double cosMeanLatitude = Math.cos(((starLat + endLat) / 2) * toRadians);
                    double second = Math.pow(cosMeanLatitude * deltaLong, 2);
                    double distance = radius * Math.sqrt(deltaLat + second);
                    ZonedDateTime startDateTime;
                    ZonedDateTime endDateTime;
                    try{
                        dateStart=dateStart.substring(1,dateStart.length()-1);
                        dateEnd=dateEnd.substring(1,dateEnd.length()-1);
                        startDateTime = ZonedDateTime.of(LocalDateTime.parse(dateStart,formatter), zoneId);
                        endDateTime = ZonedDateTime.of(LocalDateTime.parse(dateEnd,formatter), zoneId);
                    }catch (DateTimeParseException e){
                        System.out.println("**Bad date format "+value.toString());
                        return;
                    }


                    float duration = Duration.between(startDateTime, endDateTime).getSeconds()/3600f;
                    long startDateMillis=startDateTime.toInstant().toEpochMilli();
                    double speed=distance/duration;  /// Units: KM/hrs

                    Text infoNew= new Text(""+starLat+"&"+starLong+"&"+endLat+"&"+endLong+"&"+speed+"&"+statusStart+"&"+statusEnd);
                    TaxiIDDatePair i=new TaxiIDDatePair(id,startDateMillis);
                    context.write(i,infoNew);
                    //if (speed < 200) {
                    //    return distance;
                    //} else {
                    //    System.out.println("!!--Speed overeceed: " );
                    //}
                }else{
                    System.out.println(" <Point out of US map>");
                }

            }else{
                System.out.println("Invalid size fields: "+value.toString());
            }



        }
    }

    public  class TaxiIDDateGroupingComparator extends WritableComparator {

        public TaxiIDDateGroupingComparator() {
            super(TaxiIDDatePair.class, true);
        }

        @Override
        /**
         * This comparator controls which keys are grouped
         * together into a single call to the reduce() method
         */
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            TaxiIDDatePair pair = (TaxiIDDatePair) wc1;
            TaxiIDDatePair pair2 = (TaxiIDDatePair) wc2;
            return pair.getTaxiID().compareTo(pair2.getTaxiID());
        }
    }

    public static class TaxiDriverReducer
            extends Reducer<TaxiIDDatePair, Text, IntWritable, Text> {

        @Override
        public void reduce(TaxiIDDatePair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("En el reducer");
            String test = "";
            for (Text value : values) {
                test +=value;
            }
            System.out.println(key.getTaxiID()+"  "+test);
            context.write(key.getTaxiID(), new Text(test));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) ) {
            System.err.println("Usage: We need <input path> <output path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(TaxiRecords.class);
//        job.setMapperClass(TaxiDriverMapper.class);
//        job.setReducerClass(TaxiDriverReducer.class);
//        job.setOutputKeyClass(TaxiIDDatePair.class);
//        job.setOutputValueClass(Text.class);
//        job.setGroupingComparatorClass(TaxiIDDateGroupingComparator.class);

        job.setMapperClass(TaxiRecords.TaxiDriverMapper.class);
        //job.setCombinerClass(TaxiRecords.TaxiDriverReducer.class);
        job.setReducerClass(TaxiRecords.TaxiDriverReducer.class);

        job.setMapOutputKeyClass(TaxiIDDatePair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        //job.setGroupingComparatorClass(TaxiIDDateGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+System.currentTimeMillis()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


//        Path input = new Path(args[0]);
//        Path output1 = new Path(args[1], "pass1");
//
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(TaxiRecords.class);



        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));



        //job.setPartitionerClass(TemperaturePartitioner.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


        // subsection 1.1 - first map reduce job
//        Job wordcountJob = runWordCount(input, output1);
//        if (!wordcountJob.waitForCompletion(true)) {
//            System.exit(1);
//        }


    }
}