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
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

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

    public void setTaxiID(int otaxiID) {
        taxiID.set(otaxiID);
    }

    public LongWritable getStartDateMillis() {
        return startDateMillis;
    }

    public void setStartDateMillis(long ostartDateMillis) {
        startDateMillis.set( ostartDateMillis);
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
    static Logger logger = Logger.getLogger(TaxiRecords.class);

    private static final String EMPTY_STATUS = "'E'";
    private static final String CLIENT_STATUS = "'M'";
    private static final float SPEED_LIMIT= 200f;


    /* --- subsection 1.1 and 1.2 ------------------------------------------ */
    public static class TaxiDriverMapper
            extends Mapper<LongWritable, Text, TaxiIDDatePair, Text> {



        static final float radius=6371.009f;
        static final float toRadians= (float)(Math.PI / 180);
        static final float top = 49.3457868f ;// north lat
        static final float left = -124.7844079f;// west long
        static final float right = -66.9513812f; // east long
        static final float bottom =  24.7433195f; // south lat

        static final ZoneId zoneId = ZoneId.of("America/Los_Angeles");
        static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        private ZonedDateTime startDateTime;
        private ZonedDateTime endDateTime;
        String dateStart;
        String dateEnd;
        Text infoNew= new Text();
        TaxiIDDatePair newKey=new TaxiIDDatePair();





        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line=value.toString();
            String[] info = line.split(",");
            //706,'2010-02-28 23:46:08',37.66721,-122.41029,'E','2010-03-01 04:02:28',37.6634,-122.43123,'E'
            //System.out.println(info.length);
            if (info.length>=9) {
                if(info.length>9)
                    System.out.println("[Exceeded] "+line);


                int id = Integer.parseInt(info[0]);
                dateStart = info[1];
                float starLat = 0;
                float starLong = 0;

                dateEnd = info[info.length - 4];
                float endLat=0;
                float endLong=0;
                try{
                    starLat = Float.parseFloat(info[2]);
                    starLong = Float.parseFloat(info[3]);
                    endLat = Float.parseFloat(info[info.length - 3]);
                    endLong =Float.parseFloat( info[info.length - 2]);
                }catch (Exception e){
                    System.out.println("Error parsing latitudes: "+line);
                   return;
                }
                String statusStart = info[4];
                //statusStart=statusStart.substring(1,2);
                String statusEnd = info[info.length - 1];
                //statusEnd=statusEnd.substring(1,2);
                if(statusStart.equals(EMPTY_STATUS)&& statusEnd.equals(EMPTY_STATUS)){
                    //System.out.println("[Skip empty track]");
                    return;
                }



                //if  ((bottom<starLat && starLat<top )&& (bottom<endLat&&endLat<top )&&(left<starLong&&starLong<right)&&(left<endLong&&endLong<right)){
                    double deltaLat = Math.pow((starLat - endLat) * toRadians, 2f);
                    double deltaLong = (starLong - endLong) * toRadians;
                    double cosMeanLatitude = Math.cos(((starLat + endLat) / 2) * toRadians);
                    double second = Math.pow(cosMeanLatitude * deltaLong, 2);
                    double distance = radius * Math.sqrt(deltaLat + second);

                    try{
                        dateStart=dateStart.substring(1,dateStart.length()-1);
                        dateEnd=dateEnd.substring(1,dateEnd.length()-1);
                        startDateTime = ZonedDateTime.of(LocalDateTime.parse(dateStart,formatter), zoneId);
                        endDateTime = ZonedDateTime.of(LocalDateTime.parse(dateEnd,formatter), zoneId);
                    }catch (DateTimeParseException e){
                        System.out.println("**Bad date format "+line);
                        return;
                    }


                    float duration = Duration.between(startDateTime, endDateTime).getSeconds()/3600f;
                    //long startDateMillis=startDateTime.toInstant().toEpochMilli();
                    long startDateUnix=startDateTime.toEpochSecond();
                    double speed=distance/duration;  /// Units: KM/hrs

                    infoNew.set(id+","+startDateUnix+","+info[2]+","+info[3]+","+statusStart+","+endDateTime.toEpochSecond()+","+info[info.length - 3]+","+info[info.length - 2]+","+statusEnd+","+speed);

                    //infoNew.set(id+","+st+speed);
                    //TaxiIDDatePair newKey=new TaxiIDDatePair(id,startDateMillis);

                    if(statusStart.equals(CLIENT_STATUS)&& statusEnd.equals(CLIENT_STATUS)&&speed<SPEED_LIMIT){
                        //System.out.println("[Skip empty track]");
                        return;
                    }
                    newKey.setTaxiID(id);
                    newKey.setStartDateMillis(startDateUnix);
                    context.write(newKey,infoNew);
                    //if (speed < 200) {
                    //    return distance;
                    //} else {
                    //    System.out.println("!!--Speed overeceed: " );
                    //}
                }else{
                    System.out.println("Invalid size fields: "+line);
                }

//            }else{
//                System.out.println("Invalid size fields: "+line);
//            }



        }
    }

    public class KeyPartitioner extends Partitioner<TaxiIDDatePair, Text> {
        @Override
        public int getPartition(TaxiIDDatePair key, Text value, int numPartitions) {

            // Automatic n-partitioning using hash on the state name
            return Math.abs(key.getTaxiID().hashCode() ) % numPartitions;
        }
    }



    public static class TaxiIDDateGroupingComparator extends WritableComparator {

        public TaxiIDDateGroupingComparator() {
            super(TaxiIDDatePair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {

            TaxiIDDatePair key1 = (TaxiIDDatePair) wc1;
            TaxiIDDatePair key2 = (TaxiIDDatePair) wc2;
            return key1.getTaxiID().compareTo(key2.getTaxiID());
        }
    }





    public static class TaxiDriverReducer
            extends Reducer<TaxiIDDatePair, Text, IntWritable, Text> {
        Text result = new Text();
        String resultS="";
        String line;
        String [] info;
        String dateStart,dateEnd;
        public final int offsetNewParameters=1;
        String starLat,starLong,endLat,endLong;
        @Override
        public void reduce(TaxiIDDatePair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            line="";
            //System.out.println("En el reducer");
            resultS="";
            boolean startedTrip=false;



            for (Text value : values) {
                line=value.toString();
                info=line.split(",");
                //System.out.println(info.length);
                //System.out.println(line);
                String statusStart = info[4];
                //statusStart=statusStart.substring(1,2);
                String statusEnd = info[info.length - 1 -offsetNewParameters];
                //statusEnd=statusEnd.substring(1,2);
                float speed=Float.parseFloat(info[info.length-1]);
                if(statusStart.equals(EMPTY_STATUS)&&statusEnd.equals(CLIENT_STATUS)&&!startedTrip){
                    startedTrip=true;
                    //dateStart = info[1];
                    //starLat = info[2];
                    //starLong = info[3];
                    dateStart = info[info.length - 4 -offsetNewParameters];
                    starLat = info[info.length - 3-offsetNewParameters];
                    starLong =info[info.length - 2-offsetNewParameters];
                }else if(statusStart.equals(CLIENT_STATUS)&&statusEnd.equals(EMPTY_STATUS)&&startedTrip){
                    //dateEnd = info[info.length - 4 -offsetNewParameters];
                    //endLat = info[info.length - 3-offsetNewParameters];
                    //endLong =info[info.length - 2-offsetNewParameters];

                    dateEnd = info[1];
                    endLat = info[2];
                    endLong = info[3];

                    resultS=dateStart+" "+starLat+" "+starLong+" "+dateEnd+" "+endLat+" "+endLong;
                    result.set(resultS);
                    context.write(key.getTaxiID(),result);
                    startedTrip=false;
                }else if(speed>=SPEED_LIMIT&&statusStart.equals(CLIENT_STATUS)&&statusEnd.equals(CLIENT_STATUS)&&startedTrip){
                    startedTrip=false;
                }else if(startedTrip&&statusStart.equals(EMPTY_STATUS)&&statusEnd.equals(CLIENT_STATUS)){  //Discard info from previus missing star trip
                    startedTrip=true;
                    //dateStart = info[1];
                    //starLat = info[2];
                    //starLong = info[3];

                    dateStart = info[info.length - 4 -offsetNewParameters];
                    starLat = info[info.length - 3-offsetNewParameters];
                    starLong =info[info.length - 2-offsetNewParameters];
                    //System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ " +key.getTaxiID());
                }



            }
            //result.set(resultS);
            //System.out.println(key.getTaxiID().hashCode());
            //System.out.println(key.getTaxiID()+" "+key.getStartDateMillis()+"  "+result);
            //context.write(key.getTaxiID(),result);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        //conf.setBoolean("mapreduce.map.output.compress", true);
        //conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
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
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TaxiIDDateGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/"+System.currentTimeMillis()));
        //FileOutputFormat.setCompressOutput(job, true);
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
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