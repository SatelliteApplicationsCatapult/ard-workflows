#!/bin/bash
INDIR=$1
ZIPDIR=$2
OUTDIR=$3
OLDINFILE=$4
NEWINFILE=$5
nargs=$#
if [ $nargs -ne 5 ]; then
        echo "Incorrect syntax, correct syntax is ippcolombia_processing.sh inputdirectory zipfiledirectory outputfiledirectory olderinputfile newerinputfile"
        exit
fi


# INDIR is the input directory, zipdir is where symbolic links are created to the ziped downloaded files, outdir is where the output files are piped to. 

# Script created by Electra Panagoulia, starting on 6/6/2017, to process SAR data. Based on the scripts created for the Eyes on the Sea project by Andrea Minchella.
############ Change log
# 6/6/2017: First version of the script, which handles just S1 IW GRDH data files.
# 13/6/2017: At the request of Dan Clewley, the ML step was removed and the ellipsoid correction was replaced by terrain correction.
# 9/8/2017: Changes made to this script and the crontab script, so that the crontab script passes the exact filenames that need processing to the processing script.
# 10/8/2017: Thermal noise removal and GRD border noise removal (this change is actually in the S1 xml chain being called).
# 6/9/2017: Splitting polarisations step removed to allow GRD border noise removal to work.
# 05/10/2017: Work started on transforming the EASOS processing script into a script that would process any S1 GRD data (have yet to find an IW_GRDM image to test this on). Things to fix: do we need ML? Process all files in a given input directory. User-defined output directory. Ellipsoid/terrain correction? Directories where SNAP and all the xml files are kept. 
# Project Sausage! A.k.a. IPP Colombia. Script that will make the processing pipeline run automatically across a pair of images. 


##directory for input SAR imagery
echo "The input SAR image directory is: $INDIR"

export OUTDIR=$OUTDIR/Output_products   ##directory for SAR derived output products
echo "The output SAR image directory is: $OUTDIR"
mkdir $OUTDIR


#export ZIPDIR=/mnt/Malaysia_EASOS/maritime/S1/Processed_Input_data/ZIPFILE   ##directory where zip input data are moved

export GRAPHSDIR_SNAP=/home/cristian/IPPColombia ## PARENT directory containing the chains (xml) for the processing with SNAP.

export SNAP_HOME=/home/cristian/snap/bin  ##directory of installation for the SNAP toolbox
echo $SNAP_HOME
export logfile=$OUTDIR/logfile.txt  ### Generation of logfile for the processing

export Inputfile=$OUTDIR/Input_list.txt  ### Generation of a list of input data processed


echo -n "Processing started on " >> $logfile
date +"%r, %A, %B %-d, %Y" >> $logfile
echo \ >> $logfile
echo -n "The input SAR image directory is: $INDIR" >> $logfile
echo \ >> $logfile
echo -n "The output SAR image directory is: $OUTDIR" >> $logfile
echo \ >> $logfile
echo -n "Directory where INPUT data are moved at the end of the processing: $PRODIR" >> $logfile
echo \ >> $logfile
echo -n "Graph chains folder: $GRAPHSDIR_SNAP" >> $logfile
echo \ >> $logfile

#cd $INDIR
#ls -1 $INDIR | grep GRDH  >> $Inputfile
#echo -n "List of SAR images processed" >> $logfile
#echo \ >> $logfile
#cat $Inputfile >> $logfile
#echo \ >> $logfile

#first of all, go to input directory and unzip zip files, in this instance we will not move zip files to processed input directory but we will create a symlink instead (to save space).
cd $INDIR
unzip $OLDINFILE".zip"
unzip $NEWINFILE".zip"
ln -s $INDIR/$i $ZIPDIR/$i


#now, for each S1 data directory in the input data directory, carry out the processing
#the rest of this process should not need to change if we're processing files one by one, as only just-downloaded zip files should exist in .SAFE format in the input directory after being unzipped
# this for-loop and the one above could be combined into one, if needs be

echo $OLDINFILE
MISSION_SAR1=`echo $OLDINFILE | cut -c 1-3`
echo -n "Product processing started at"
date +"%r, %A, %B %-d, %Y"

echo -n "Product processing started at" >> $logfile
date +"%r, %A, %B %-d, %Y" >> $logfile
echo \ >> $logfile

echo "Mission: Sentinel-1 Satellite" >> $logfile
SAR_MODE1=`echo $OLDINFILE | cut -c 5-6`
echo "SAR Mode: $SAR_MODE" >> $logfile
PRODUCT_TYPE1=`echo $OLDINFILE | cut -c 8-16`
echo "Product type: $PRODUCT_TYPE" >> $logfile
echo "${SAR_MODE}_${PRODUCT_TYPE}"
PRODUCT_DATE1=`echo $OLDINFILE | cut -c 18-48`
echo "Date and time of acquisition: $PRODUCT_DATE" >> $logfile
PRODUCT_DIR1=$INDIR/$OLDINFILE".SAFE"
echo "Product folder: $PRODUCT_DIR" >> $logfile
echo "Product xml file: ${PRODUCT_DIR}/manifest.safe" #>> $logfile

START_TIME1=`echo $OLDINFILE | cut -c 18-32`
STOP_TIME1=`echo $OLDINFILE | cut -c 34-48`

START_TIME_YEAR1=`echo $OLDINFILE | cut -c 18-21`
START_TIME_MONTH1=`echo $OLDINFILE | cut -c 22-23`
START_TIME_DAY1=`echo $OLDINFILE | cut -c 24-25`
START_TIME_HOUR1=`echo $OLDINFILE | cut -c 26-28`
START_TIME_MIN1=`echo $OLDINFILE | cut -c 29-30`
START_TIME_SEC1=`echo $OLDINFILE | cut -c 31-32`
STOP_TIME_YEAR1=`echo $OLDINFILE | cut -c 34-37`
STOP_TIME_MONTH1=`echo $OLDINFILE | cut -c 38-39`
STOP_TIME_DAY1=`echo $OLDINFILE | cut -c 40-41`
STOP_TIME_HOUR1=`echo $OLDINFILE | cut -c 42-44`
STOP_TIME_MIN1=`echo $OLDINFILE | cut -c 45-46`
STOP_TIME_SEC1=`echo $OLDINFILE | cut -c 47-48`



echo $NEWINFILE
MISSION_SAR2=`echo $NEWINFILE | cut -c 1-3`
echo -n "Product processing started at"
date +"%r, %A, %B %-d, %Y"

echo -n "Product processing started at" >> $logfile
date +"%r, %A, %B %-d, %Y" >> $logfile
echo \ >> $logfile

echo "Mission: Sentinel-1 Satellite" >> $logfile
SAR_MODE2=`echo $NEWINFILE | cut -c 5-6`
echo "SAR Mode: $SAR_MODE" >> $logfile
PRODUCT_TYPE2=`echo $NEWINFILE | cut -c 8-16`
echo "Product type: $PRODUCT_TYPE" >> $logfile
echo "${SAR_MODE}_${PRODUCT_TYPE}"
PRODUCT_DATE2=`echo $NEWINFILE | cut -c 18-48`
echo "Date and time of acquisition: $PRODUCT_DATE" >> $logfile
PRODUCT_DIR2=$INDIR/$NEWINFILE".SAFE"
echo "Product folder: $PRODUCT_DIR" >> $logfile
echo "Product xml file: ${PRODUCT_DIR}/manifest.safe" #>> $logfile

START_TIME2=`echo $NEWINFILE | cut -c 18-32`
STOP_TIME2=`echo $NEWINFILE | cut -c 34-48`

START_TIME_YEAR2=`echo $NEWINFILE | cut -c 18-21`
START_TIME_MONTH2=`echo $NEWINFILE | cut -c 22-23`
START_TIME_DAY2=`echo $NEWINFILE | cut -c 24-25`
START_TIME_HOUR2=`echo $NEWINFILE | cut -c 26-28`
START_TIME_MIN2=`echo $NEWINFILE | cut -c 29-30`
START_TIME_SEC2=`echo $NEWINFILE | cut -c 31-32`
STOP_TIME_YEAR2=`echo $NEWINFILE | cut -c 34-37`
STOP_TIME_MONTH2=`echo $NEWINFILE | cut -c 38-39`
STOP_TIME_DAY2=`echo $NEWINFILE | cut -c 40-41`
STOP_TIME_HOUR2=`echo $NEWINFILE | cut -c 42-44`
STOP_TIME_MIN2=`echo $NEWINFILE | cut -c 45-46`
STOP_TIME_SEC2=`echo $NEWINFILE | cut -c 47-48`

		

echo $PRODUCT_DIR1 $PRODUCT_DIR2

# for IPP Colombia, we are dealing with IW SLC products
#first of all, check if final output files exist (i.e. processing has already been done)

finaloutput_vh=$START_TIME1"_"$START_TIME2"_Orb_Stack_Ifg_Deb_mrg_DInSAR_Flt_TC_vh.dim"
finaloutput_vv=$START_TIME1"_"$START_TIME2"_Orb_Stack_Ifg_Deb_mrg_DInSAR_Flt_TC_vv.dim"

if [ -f $finaloutput_vh ] && [ -f $finaloutput_vv ]; then 
	echo "Both final output files exist, so processing has been done. Exiting..."
	exit
	else
	#carry out step 1 (splitting of each image into polarisations and swaths)
	    OUTPUT_FOLDER=$OUTDIR/$START_TIME1"_"$START_TIME2
	    echo $OUTPUT_FOLDER
	
	$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_coherence_step1_mod_first.xml -Pinput1=$PRODUCT_DIR1/manifest.safe \
	-Ptarget1=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vh.dim" \
	-Ptarget2=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vh.dim" \
	-Ptarget3=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vh.dim" \
	-Ptarget4=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vv.dim" \
	-Ptarget5=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vv.dim" \
	-Ptarget6=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vv.dim" \
	
	#OUTPUT_FOLDER=$OUTDIR/$START_TIME1"_"$START_TIME2
	$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_coherence_step1_mod_first.xml -Pinput1=$PRODUCT_DIR2/manifest.safe \
	-Ptarget1=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vh.dim" \
	-Ptarget2=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vh.dim" \
	-Ptarget3=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vh.dim" \
	-Ptarget4=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vv.dim" \
	-Ptarget5=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vv.dim" \
	-Ptarget6=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vv.dim" \
	
fi
	
#carry out step 2 for each pair of files (interferogram creation)
$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw1_vh.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw1_vh.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vh.dim

"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw2_vh.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw2_vh.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vh.dim

"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw3_vh.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw3_vh.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vh.dim
	
"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw1_vv.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw1_vv.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vv.dim	
	
"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw2_vv.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw2_vv.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vv.dim	
	
"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step2_mod.xml -Pinput2=${OUTPUT_FOLDER}/${MISSION_SAR1}_${SAR_MODE1}_${PRODUCT_TYPE1}_${PRODUCT_DATE1}_Orb_iw3_vv.dim -Pinput3=${OUTPUT_FOLDER}/${MISSION_SAR2}_${SAR_MODE2}_${PRODUCT_TYPE2}_${PRODUCT_DATE2}_Orb_iw3_vv.dim \
-Ptarget7=$OUTPUT_FOLDER/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vv.dim	
	
# carry out step 3 (merging all swaths from each polarisation)
"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step3_mod.xml -Pinput4=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vh.dim -Pinput5=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vh.dim \
-Pinput6=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vh.dim -Ptarget8=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vh.dim

"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step3_mod.xml -Pinput4=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vv.dim -Pinput5=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vv.dim \
-Pinput6=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vv.dim -Ptarget8=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vv.dim

#carry out step 4 (filtering, terrain correction)
"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step4.xml -Pinput7=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vh.dim -Ptarget9=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_DInSAR_Flt_TC_vh.dim

"$SNAP_HOME"/gpt $GRAPHSDIR_SNAP/S1_coherence_step4.xml -Pinput7=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vv.dim -Ptarget9=${OUTPUT_FOLDER}/${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_DInSAR_Flt_TC_vv.dim

#we've done the coherence processing bit, now for the intensity processing bit
#step 1 of intensity processing for both input files
$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_intensity_step1_mod.xml -Pinput8=$PRODUCT_DIR1/manifest.safe -Ptarget10=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_Cal_Deb_ML.dim"
$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_intensity_step1_mod.xml -Pinput8=$PRODUCT_DIR2/manifest.safe -Ptarget10=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_Cal_Deb_ML.dim"

#step 2 of intensity processing for both input files
$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_intensity_step2_mod.xml -Pinput9=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_Cal_Deb_ML.dim" -Ptarget11=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_Cal_Deb_ML_Spk_TC_dB_vh.dim" -Ptarget12=$OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_Cal_Deb_ML_Spk_TC_dB_vv.dim"
$SNAP_HOME/gpt $GRAPHSDIR_SNAP/S1_intensity_step2_mod.xml -Pinput9=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_Cal_Deb_ML.dim" -Ptarget11=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_Cal_Deb_ML_Spk_TC_dB_vh.dim" -Ptarget12=$OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_Cal_Deb_ML_Spk_TC_dB_vv.dim"

#big cleanup
cd $OUTPUT_FOLDER
rm -rf $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vh.dim" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vh.dim" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vh.dim" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vv.dim" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vv.dim" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vv.dim"
rm -rf $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vh.dim" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vh.dim" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vh.dim" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vv.dim" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vv.dim" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vv.dim"
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vh.dim ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vh.dim ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vh.dim 
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vv.dim ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vv.dim ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vv.dim
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vh.dim ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vv.dim

rm -rf $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vh.data" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vh.data" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vh.data" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw1_vv.data" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw2_vv.data" $MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_iw3_vv.data"
rm -rf $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vh.data" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vh.data" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vh.data" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw1_vv.data" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw2_vv.data" $MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_iw3_vv.data"
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vh.data ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vh.data ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vh.data
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw1_vv.data ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw2_vv.data ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_iw3_vv.data
rm -rf ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vh.data ${START_TIME1}_${START_TIME2}_Orb_Stack_Ifg_Deb_mrg_vv.data

rm -rf $OUTPUT_FOLDER/$MISSION_SAR1"_"$SAR_MODE1"_"$PRODUCT_TYPE1"_"$PRODUCT_DATE1"_Orb_Cal_Deb_ML.dim" $OUTPUT_FOLDER/$MISSION_SAR2"_"$SAR_MODE2"_"$PRODUCT_TYPE2"_"$PRODUCT_DATE2"_Orb_Cal_Deb_ML.dim"

echo -n "Processing ended at"
date +"%r, %A, %B %-d, %Y"

echo -n "Processing ended at" >> $logfile
date +"%r, %A, %B %-d, %Y" >> $logfile


exit