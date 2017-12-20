#!/usr/bin/perl
use strict;
use warnings FATAL => 'all';
use File::Path;
use Time::localtime;

END {
    # Executed before program exits.
    update_job_sts();
}

die "Batch number is not specified." if (!defined($ARGV[0]));

die "CMSS_HOME is not defined." if (!defined($ARGV[1]) and !defined($ENV{'CMSS_HOME'}));

my $CMSS_HOME;

if (defined($ARGV[1])) {
    $CMSS_HOME = $ARGV[1];
}
else {
    $CMSS_HOME = $ENV{'CMSS_HOME'}
}

my $WORK_DIR = "$CMSS_HOME/tmp";
my $SCRIPT_DIR = "$CMSS_HOME/sql-template";
my $LOG_DIR = "$CMSS_HOME/logs";

my %MAIN_PARAM = ();
my %JOB_PARAM = ();

$MAIN_PARAM{"HOSTNM"} = "10.97.10.200";
$MAIN_PARAM{"USERNM"} = "xiaoy";
$MAIN_PARAM{"PASSWD"} = "CMSS2017";
$MAIN_PARAM{'CMSSDB'} = "CMSSDATA";
$MAIN_PARAM{'TEMP_DB'} = 'CMSSTEMP';
$MAIN_PARAM{'LOAD_TEMPLATE_TBL'} = 'pz_fastld';
$MAIN_PARAM{"BATCH_NBR"} = $ARGV[0];

my $LOG_FILE = "$LOG_DIR/pz_$MAIN_PARAM{'BATCH_NBR'}.log";

my $g_job_sts;

if (!-e $WORK_DIR) {
    mkpath($WORK_DIR)
}
if (!-e $LOG_DIR) {
    mkpath($LOG_DIR)
}

chdir($WORK_DIR);
openLog();
main();

sub main {

    # 获得配资计算作业参数
    get_job_info($MAIN_PARAM{'BATCH_NBR'});

    # 设置作业为运行状态
    $g_job_sts = '0';
    update_job_sts();


}

sub prepare_acct_table() {
    my %PARAM = %MAIN_PARAM;
    foreach my $k (keys %JOB_PARAM) {
        $PARAM{$k} = $JOB_PARAM{$k};
    }

    if ($JOB_PARAM{'prmt_type'} eq "0") {
        $PARAM{'LOAD_TBL'} = "pz_fastld_$PARAM{'abno_incm_calc_btch'}";
        $PARAM{'LOAD_ERR_TBL_1'} = "abno_fastld_$PARAM{'abno_incm_calc_btch'}_err1";
        $PARAM{'LOAD_ERR_TBL_2'} = "abno_fastld_$PARAM{'abno_incm_calc_btch'}_err2";
        $PARAM{'LOAD_DATA_FILE'} = $JOB_PARAM{'invst_cntnt'};

        run_fastload("load-file.sql", %PARAM);
        run_update("prepare-invst-from-file.sql", %PARAM);
    }
    else {
        run_update("prepare-invst-per-type.sql", %PARAM);
    }
}

sub update_job_sts() {

    my %PARAM = %MAIN_PARAM;
    foreach my $k (keys %JOB_PARAM) {
        $PARAM{$k} = $JOB_PARAM{$k};
    }

    $PARAM{'job_sts'} = $g_job_sts;

    run_update("update-job-sts.sql", %PARAM);

}

sub get_job_info() {
    my ($batch_nbr) = @_;

    my %PARAM = %MAIN_PARAM;

    $PARAM{'LD_NBR'} = $batch_nbr;

    my @ret = run_query("get-job-info.sql", %PARAM);

    my ($ld_nbr, $s_date, $e_date, $job_s_date1, $job_s_date2,
        $job_e_date1, $job_e_date2, $job_sts, $job_usr,
        $prmt_type, $prmt_val) = split(/\s+/, $ret[0]);

    $JOB_PARAM{'ld_nbr'} = $ld_nbr;
    $JOB_PARAM{'s_date'} = $s_date;
    $JOB_PARAM{'e_date'} = $e_date;
    $JOB_PARAM{'job_s_date'} = "$job_s_date1 $job_s_date2";
    $JOB_PARAM{'job_e_date'} = "$job_e_date1 $job_e_date2";
    $JOB_PARAM{'job_sts'} = $job_sts;
    $JOB_PARAM{'job_usr'} = $job_usr;
    $JOB_PARAM{'prmt_type'} = $prmt_type;
    $JOB_PARAM{'prmt_val'} = $prmt_val;
    $JOB_PARAM{'prmt_val_quote'} = quote_values($prmt_val);

    print "Job Parameters:\n";
    foreach my $k (keys %JOB_PARAM) {
        print "\t$k = $JOB_PARAM{$k}\n";
    }
    print "\n";
}

sub quote_values() {
    my ($values) = @_;

    return "'" . join("','", split(/,/, $values)) . "'"
}

sub openLog {

    my $START_TIME = getTime("yyyy-mm-dd hh:mi:ss");

    open(STDOUT, ">$LOG_FILE")
        or die "Can not redirect STDOUT to $LOG_FILE";
    print "Standard output goes to this file at $START_TIME\n";

    open(STDERR, ">&STDOUT")
        or die "Can not redirect STDERR to $LOG_FILE";
    print STDERR "Standard error goes to this file at $START_TIME\n";

}

sub run_fastload() {
    my ($template, %PARAM) = @_;

    open(my $template_fh, "$SCRIPT_DIR/$template") or die "Can not open $SCRIPT_DIR/$template";

    my $template_script = "";
    while (<$template_fh>) {
        $template_script .= $_;
    }
    close($template_fh);

    print "TEMPLATE BEGIN >>>>>>\n$template_script\n<<<<<< TEMPLATE END\n";

    my $complete_script = eval("return \"$template_script\"");

    #    print "After process ...\n$complete_script\n";

    open(my $complete_script_fh, ">$template.fld") or die "Can not open $template.fld";
    print $complete_script_fh $complete_script;
    close($complete_script_fh);

    print "Running script ...\n";

    sleep(0.1);

    my $ret = system("fastload < $template.fld");

    die "Failed to execute FASTLOAD script $template.fld." if ($ret != 0);

}

sub run_query() {
    my ($template, %PARAM) = @_;

    $PARAM{'EXPORT_OUTPUT'} = "$template.out";

    open(my $template_fh, "$SCRIPT_DIR/$template") or die "Can not open $SCRIPT_DIR/$template";

    my $template_script = "";
    while (<$template_fh>) {
        $template_script .= $_;
    }
    close($template_fh);

    print "TEMPLATE BEGIN >>>>>>\n$template_script\n<<<<<< TEMPLATE END\n";

    my $complete_script = eval("return \"$template_script\"");

    #    print "After process ...\n$complete_script\n";

    open(my $complete_script_fh, ">$template.bteq") or die "Can not open $template.bteq";
    print $complete_script_fh $complete_script;
    close($complete_script_fh);

    print "Running script ...\n";

    unlink($PARAM{'EXPORT_OUTPUT'});

    sleep(0.1);

    my $ret = system("bteq < $template.bteq");

    die "Failed to execute BTEQ script $template.bteq." if ($ret != 0);

    open(my $output_fh, $PARAM{'EXPORT_OUTPUT'})
        or die "Can not open output file $PARAM{'EXPORT_OUTPUT'}";

    my @output = ();
    my $row = 0;
    while (<$output_fh>) {
        next if (++$row) <= 2;
        chomp;
        $output[++$#output] = $_ if (length($_) > 0);
    }
    close($output_fh);

    return @output;
}

sub run_update() {
    my ($template, %PARAM) = @_;

    open(my $template_fh, "$SCRIPT_DIR/$template") or die "Can not open $SCRIPT_DIR/$template";

    my $template_script = "";
    while (<$template_fh>) {
        $template_script .= $_;
    }
    close($template_fh);

    print "TEMPLATE BEGIN >>>>>>\n$template_script\n<<<<<< TEMPLATE END\n";

    my $complete_script = eval("return \"$template_script\"");

    #    print "After process ...\n$complete_script\n";

    open(my $complete_script_fh, ">$template.bteq") or die "Can not open $template.bteq";
    print $complete_script_fh $complete_script;
    close($complete_script_fh);

    print "Running script ...\n";

    sleep(0.1);

    my $ret = system("bteq < $template.bteq");

    die "Failed to execute BTEQ script $template.bteq." if ($ret != 0);
}

#取系统时间
sub getTime {

    my ($ret) = @_;            #获取时间格式

    my $tc = localtime(time());   #获取当前时间$tc存储的是内存地址
    $tc = sprintf("%4d%02d%02d%02d%02d%02d", $tc->year + 1900, $tc->mon + 1,
        $tc->mday, $tc->hour, $tc->min, $tc->sec);                    #将时间拼为字符串

    #进行格式化
    my $tmp = substr($tc, 0, 4);
    $ret =~ s/YYYY/$tmp/gi;

    $tmp = substr($tc, 4, 2);
    $ret =~ s/MM/$tmp/gi;

    $tmp = substr($tc, 6, 2);
    $ret =~ s/DD/$tmp/gi;

    $tmp = substr($tc, 8, 2);
    $ret =~ s/HH/$tmp/gi;

    $tmp = substr($tc, 10, 2);
    $ret =~ s/MI/$tmp/gi;

    $tmp = substr($tc, 12, 2);
    $ret =~ s/SS/$tmp/gi;

    return $ret;
}