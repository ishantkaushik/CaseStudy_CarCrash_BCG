from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
import utils

def crashes_with_males_killed(df_primary_person, output_path, output_format):
        """
        Analysis 1
        Finds the crashes (accidents) in which number of persons killed are male
        """
        df = df_primary_person.filter(df_primary_person.PRSN_GNDR_ID == "MALE")
        utils.write_output(df, output_path, output_format)
        return df.count()

def crashes_count_two_wheeler_accidents(df_units, output_path, output_format):
        """
        Analysis 2
        Finds the crashes where the vehicle type was 2 wheeler.
        """
        df = df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        utils.write_output(df, output_path, output_format)

        return df.count()

def get_statesname_with_highest_female_accidents(df_primary_person, output_path, output_format):
        """
        Analysis 3
        Finds state name with highest female accidents
        """
        df = df_primary_person.filter(df_primary_person.PRSN_GNDR_ID == "FEMALE"). \
            groupby("DRVR_LIC_STATE_ID").count(). \
            orderBy(col("count").desc())
        utils.write_output(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

def get_top_5to15_vehicle_with_highestinjuries(df_units, output_path, output_format):
        """
        Analysis 4
        Finds Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        df = df_units.filter(df_units.VEH_MAKE_ID != "NA"). \
            withColumn('TOT_CASUALTIES_CNT', df_units[35] + df_units[36]). \
            groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT"). \
            withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
            orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())

        df_top_5_to_15 = df.limit(15).subtract(df.limit(5))
        utils.write_output(df_top_5_to_15, output_path, output_format)

        return [veh[0] for veh in df_top_5_to_15.select("VEH_MAKE_ID").collect()]

def get_ethnic_ug_crash_bodystyle_crashed(df_units,df_primary_person, output_path, output_format):
        """
        Analysis 5
        Finds and show top ethnic user group of each unique body style that was involved in crashes
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            filter(~df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row", row_number().over(w)).filter(col("row") == 1).drop("row", "count")

        utils.write_output(df, output_path, output_format)

        df.show(truncate=False)

def get_top5_zips_with_alcohols_as_crashresult(df_units,df_primary_person, output_path, output_format):
        """
        Analysis 6
        Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        """
        df = df_units.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        utils.write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

def get_uniquecrash_ids_with_nodamage(df_units,df_damages, output_path, output_format):
        """
        Analysis 7
        Counts Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        """
        df = df_damages.join(df_units, on=["CRASH_ID"], how='inner'). \
            filter(
            (
                    (df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    (~df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                    (df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    (~df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
        ). \
            filter(df_damages.DAMAGED_PROPERTY == "NONE"). \
            filter(df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        utils.write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]

def get_top5_vehicle_make(df_units,df_primary_person,df_charges, output_path, output_format):
        """
        Analysis 8
        Determines the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences
        """
        top_25_state_list = [row[0] for row in df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in df_units.filter(df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        df = df_charges.join(df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(df_units, on=['CRASH_ID'], how='inner'). \
            filter(df_charges.CHARGE.contains("SPEED")). \
            filter(df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        utils.write_output(df, output_path, output_format)

        return [row[0] for row in df.collect()]