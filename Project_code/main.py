import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession, Row,Window
from pyspark.sql.functions import *
from utilities import utils

class CrashAnalysis:
    def __init__(self, path_to_config_file):
        input_file_paths = utils.read_json(config_file_path)['INPUT_FILENAME']
        self.df_charges = utils.read_csv_to_df(spark, input_file_paths["Charges"])
        self.df_damages = utils.read_csv_to_df(spark, input_file_paths["Damages"])
        self.df_endorse = utils.read_csv_to_df(spark, input_file_paths["Endorse"])
        self.df_primary_person = utils.read_csv_to_df(spark, input_file_paths["Primary_Person"])
        self.df_units = utils.read_csv_to_df(spark, input_file_paths["Units"])
        self.df_restrict = utils.read_csv_to_df(spark, input_file_paths["Restrict"])
    def fetch_count_male_accidents(self, output_path):
        """
        1.Fetch number of crashes  in which number of persons killed are male
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "MALE").groupby().count()
    
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 1 :",[row[0] for row in df.collect()][0])
    def fetch_count_2_wheeler_accidents(self, output_path):
        """
        2.Fetch number of crashes where the vehicle type was 2 wheeler.
        """
        
        df = self.df_units.filter(self.df_units.VEH_BODY_STYL_ID.contains("MOTORCYCLE")).groupby().count()
        
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 2 :",[row[0] for row in df.collect()][0])
    def fetch_state_with_highest_female_accident(self, output_path):
        """
        3.Fetch state name with Most accidents in which female are involved
        """
        
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "FEMALE"). \
                                    groupby("DRVR_LIC_STATE_ID").count(). \
                                    sort(col("count").desc()). \
                                    limit(1). \
                                    select("DRVR_LIC_STATE_ID")
        
        utils.write_output_in_parquet(df,output_path)
        print("Output for Analysis 3 :",df.first().DRVR_LIC_STATE_ID)
    def fetch_top_vehicle_contributing_to_injuries(self, output_path):
        """
        4.Fetch Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        
        df = self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA"). \
                           withColumn('TOT_CASUALTIES_CNT', self.df_units.TOT_INJRY_CNT+self.df_units.DEATH_CNT). \
                           groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT"). \
                           withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
                           sort(col("TOT_CASUALTIES_CNT_AGG").desc()).\
                           limit(15).select("VEH_MAKE_ID")
        df_final=df.subtract(df.limit(5))
        
        utils.write_output_in_parquet(df_final, output_path)
        print("Output for Analysis 4 :",[ID[0] for ID in df_final.collect()])
    def fetch_top_ethnic_ug_crash_for_each_body_style(self, output_path):
        """
        5.top ethnic user group of each unique body style that was involved in crashes
        """
        
      
        
        df = self.df_units.join(self.df_primary_person, self.df_units.CRASH_ID == self.df_primary_person.CRASH_ID, how='inner'). \
                           filter(~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)"])). \
                           filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
                           groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
                           withColumn("row", row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()))). \
                           filter(col("row") == 1). \
                           drop("row", "count")
        
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 5 :")
        df.show(truncate=False)
    def fetch_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path):
        """
        6.Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        """
        df = self.df_units.join(self.df_primary_person,self.df_units.CRASH_ID == self.df_primary_person.CRASH_ID, how='inner'). \
                           dropna(subset=["DRVR_ZIP"]). \
                           filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
                           groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5).select("DRVR_ZIP")
        
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 6 :",[row[0] for row in df.collect()])
    def fetch_crash_ids_with_no_damage(self, output_path):
        """
        7.Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        """
        df = self.df_damages.join(self.df_units, self.df_units.CRASH_ID == self.df_damages.CRASH_ID, how='inner'). \
                            filter(((self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &(~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) |
                                   ((self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &(~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))). \
                            filter(self.df_damages.DAMAGED_PROPERTY == "NONE"). \
                            filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE"). \
                            select(self.df_units.CRASH_ID).distinct()
        
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 7 :",[row[0] for row in df.collect()])
    def fetch_top_5_vehicle_brand(self, output_path):
        """
        8.Determines the Top 5 Vehicle Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        """
        df_top_25_state=self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()). \
                                      groupby("VEH_LIC_STATE_ID").count(). \
                                      orderBy(col("count").desc()).limit(25)
        list_top_25_state = [row[0] for row in df_top_25_state.collect()]
        
        df_top_10_vehicle_colors = self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA"). \
                                                 groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10)
        list_top_10_vehicle_colors = [row[0] for row in df_top_10_vehicle_colors.collect()]

        
        df = self.df_charges.join(self.df_primary_person, self.df_primary_person.CRASH_ID == self.df_charges.CRASH_ID, how='inner'). \
                             join(self.df_units, self.df_units.CRASH_ID == self.df_charges.CRASH_ID, how='inner'). \
                             filter(self.df_charges.CHARGE.contains("SPEED")). \
                             filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
                             filter(self.df_units.VEH_COLOR_ID.isin(list_top_10_vehicle_colors)). \
                             filter(self.df_units.VEH_LIC_STATE_ID.isin(list_top_25_state)). \
                             groupby("VEH_MAKE_ID").count(). \
                             orderBy(col("count").desc()).limit(5).select("VEH_MAKE_ID")
        
        utils.write_output_in_parquet(df, output_path)
        print("Output for Analysis 8 :",[row[0] for row in df.collect()])

if __name__ == '__main__':
    spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()
    
    config_file_path = "config.json"
    
    ca_obj = CrashAnalysis(config_file_path)
    
    output_paths = utils.read_json(config_file_path)['OUTPUT_PATH']
    
    #fetch result of Analysis1
    ca_obj.fetch_count_male_accidents(output_paths['output_path_1'])
    #fetch result of Analysis2
    ca_obj.fetch_count_2_wheeler_accidents(output_paths['output_path_2'])
    #fetch result of Analysis3
    ca_obj.fetch_state_with_highest_female_accident(output_paths['output_path_3'])
    #fetch result of Analysis4
    ca_obj.fetch_top_vehicle_contributing_to_injuries(output_paths['output_path_4'])
    #fetch result of Analysis5
    ca_obj.fetch_top_ethnic_ug_crash_for_each_body_style(output_paths['output_path_5'])
    #fetch result of Analysis6
    ca_obj.fetch_top_5_zip_codes_with_alcohols_as_cf_for_crash(output_paths['output_path_6'])
    #fetch result of Analysis7
    ca_obj.fetch_crash_ids_with_no_damage(output_paths['output_path_7'])
    #fetch result of Analysis8
    ca_obj.fetch_top_5_vehicle_brand(output_paths['output_path_8'])
    spark.stop()

