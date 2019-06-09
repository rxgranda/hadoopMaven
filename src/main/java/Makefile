
JAVA_BUILD_DIR=java_build
HADOOP_INPUT=/input_text.txt
HADOOP_OUTPUT=/hadoop_output

RECORDS_TAXI=/2010_03.segments
OUTPUT_TAXI=/trips


$(JAVA_BUILD_DIR):
	mkdir -p $(JAVA_BUILD_DIR)

$(JAVA_BUILD_DIR)/WordCount.jar: WordCount.java $(JAVA_BUILD_DIR)
	javac -d $(JAVA_BUILD_DIR) -Xlint -cp $(shell $(HADOOP_INSTALL)/bin/yarn classpath) WordCount.java
	cd $(JAVA_BUILD_DIR); jar cf WordCount.jar *.class



$(JAVA_BUILD_DIR)/TaxiRecords.jar: TaxiRecords.java $(JAVA_BUILD_DIR)
	javac -d $(JAVA_BUILD_DIR) -Xlint -cp $(shell $(HADOOP_INSTALL)/bin/yarn classpath) TaxiRecords.java
	cd $(JAVA_BUILD_DIR); jar cf TaxiRecords.jar *.class



.PHONY: java_clean
java_clean:
	rm -r $(JAVA_BUILD_DIR)

.PHONY: run_hadoop
run_hadoop: $(JAVA_BUILD_DIR)/WordCount.jar
	rm -rf $(HADOOP_OUTPUT)
	$(HADOOP_INSTALL)/bin/hadoop jar $(JAVA_BUILD_DIR)/WordCount.jar WordCount $(HADOOP_INPUT) $(HADOOP_OUTPUT)


.PHONY: proyecto
proyecto: $(JAVA_BUILD_DIR)/TaxiRecords.jar
	#$(HADOOP_INSTALL)/bin/hadoop dfs -rmr $(OUTPUT_TAXI)
	$(HADOOP_INSTALL)/bin/hadoop jar $(JAVA_BUILD_DIR)/TaxiRecords.jar TaxiRecords $(RECORDS_TAXI) $(OUTPUT_TAXI)

