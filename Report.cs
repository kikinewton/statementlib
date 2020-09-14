using Oracle.DataAccess.Client;
using PdfSharp.Pdf;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Util;

namespace GenerateStatementLib
{
    public class Report
    {
        public bool GenerateStatement(string acctNo, string custId, string fromDate, string toDate, JLogger logger, System.Data.Common.DbConnection con, string password = null)
        {
            try
            {
                var mainConn = (OracleConnection)con;

                String v_mimetype;
                String v_encoding;
                String v_filename_extension;
                String[] v_streamids;
                Microsoft.Reporting.WinForms.Warning[] warnings;

                ReportViewer reportViewer1 = new ReportViewer();
                reportViewer1.LocalReport.ReportPath = @"C:\eaes\config\Report1.rdlc";
                
                reportViewer1.RefreshReport();

                DataSet ds = new DataSet();
                ds = GetCustomerInformation(custId, mainConn, fromDate, toDate);
                
                decimal runningBalance = 0;
                
                GenerateAndSaveStatementsForAllAccounts(custId, fromDate, toDate, mainConn, logger, runningBalance);
                //
                //DataSet ds2 = new DataSet();
                //ds2 = GetAccountTransactionBetweenDates(acctNo, fromDate, toDate, runningBalance, mainConn, logger);
                DataSet ds4 = new DataSet();
                ds4 = GetChannelCountsAndPercentages();
                if (ds.Tables[0].Rows.Count > 0)
                {
                    ReportDataSource rds = new ReportDataSource("DataSet1", ds.Tables[0]);
                    reportViewer1.LocalReport.DataSources.Clear();
                    reportViewer1.LocalReport.DataSources.Add(rds);

                    //now lets add the DataSet2
                    //ReportDataSource rds2 = new ReportDataSource("DataSet2", ds1.Tables[0]);
                    //reportViewer1.LocalReport.DataSources.Add(rds2);

                    //now lets add the DataSet4
                    //ReportDataSource rds4 = new ReportDataSource("DataSet4", ds3.Tables[0]);
                    //reportViewer1.LocalReport.DataSources.Add(rds4);

                    //now lets add the DataSet3
                    //ReportDataSource rds3 = new ReportDataSource("DataSet3", ds2.Tables[0]);
                    //reportViewer1.LocalReport.DataSources.Add(rds3);

                    //this last Dataset5 is for the channels with totals
                    ReportDataSource rds5 = new ReportDataSource("DataSet2", ds4.Tables[0]);
                    reportViewer1.LocalReport.DataSources.Add(rds5);

                }

                byte[] byteViewer = reportViewer1.LocalReport.Render("PDF", null, out v_mimetype, out v_encoding, out v_filename_extension, out v_streamids, out warnings);

                string path = @"C:\eaes\" + custId + "\\";
                DirectoryInfo dI = new DirectoryInfo(path);
                if (!dI.Exists) { dI.Create(); }

                //lets dispose the reportviewer1 here
                reportViewer1.Dispose();

                FileStream newFile = new FileStream(path + acctNo + "1.pdf", FileMode.Create);
                newFile.Write(byteViewer, 0, byteViewer.Length);
                newFile.Close();
                newFile.Dispose();

                //now lets loop through the path and add merge all the pdfs into 1
                DirectoryInfo d = new DirectoryInfo(path);
                PdfDocument pdfDoc = new PdfDocument();
                foreach (FileInfo f in d.GetFiles().OrderByDescending(f => f.CreationTime))
                {
                    logger.Info("file no => " + f.Name);
                    //merging the varios pdfs...
                    using (PdfDocument pd = PdfReader.Open(path + f.Name, PdfDocumentOpenMode.Import))
                    {
                        CopyPages(pd, pdfDoc);
                    }
                }
                //since we have finished merging everything, lets save it
                pdfDoc.Save(path + acctNo + ".pdf");
                pdfDoc.Close();

                //lets assume that at this time, we have created the pdf
                //we now need to put a password on the pdf file
                //i hope it does not bomb at this stage.
                PdfDocument doc = PdfReader.Open(path + acctNo + ".pdf");
                PdfSecuritySettings securitySettings = doc.SecuritySettings;
                securitySettings.UserPassword = password == null ? custId : password;
                securitySettings.OwnerPassword = password == null ? custId : password;

                //securitySettings.DocumentSecurityLevel = PdfDocumentSecurityLevel.Encrypted40Bit;

                //aditional settings
                securitySettings.PermitAccessibilityExtractContent = false;
                securitySettings.PermitAnnotations = false;
                securitySettings.PermitAssembleDocument = false;
                securitySettings.PermitExtractContent = false;
                securitySettings.PermitFormsFill = false;
                securitySettings.PermitFullQualityPrint = true;
                securitySettings.PermitModifyDocument = false;
                securitySettings.PermitPrint = true;

                doc.Save(path + acctNo + ".pdf");

                doc.Close();

                //now that we are done with the files, its time we delete all of them
                //except the merged 1
                removeOtherFiles(acctNo, custId);

                return true;
            }
            catch (Exception ex)
            {
                logger.Info(ex.Message, ex);
                return false;
            }
        }

        private DataSet GetCustomerInformation(string custId, OracleConnection mainConn, string fromDate, string toDate)
        {

            Oracle.DataAccess.Client.OracleConnection con = mainConn;
            con.Open();
            OracleCommand cmd = new OracleCommand("select * from STVW_CUST_ACCOUNTS where cust_no =" + custId + "");
           
            cmd.Connection = con;
            OracleDataReader odr = cmd.ExecuteReader();
            
            DataSet ds = new DataSet();
            

            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[11]
                    {
                        new DataColumn("CUSTOMER_NAME", typeof(string)),
                        new DataColumn("CCY", typeof(string)),
                        new DataColumn("DESCRIPTION", typeof(string)) ,
                        new DataColumn("ACY_OPENING_BAL", typeof(decimal)),
                        new DataColumn("ACY_AVL_BAL", typeof(decimal)),
                        new DataColumn("CUST_AC_NO", typeof(string)) ,
                        new DataColumn("BRANCH_NAME", typeof(string)),
                        new DataColumn("BRANCH_ADDR1", typeof(string)),
                        new DataColumn("ADDRESS2", typeof(string)) ,
                        new DataColumn("ACY_TODAY_TOVER_DR", typeof(decimal)),
                        new DataColumn("ACY_TODAY_TOVER_CR", typeof(decimal))
                    });

            while (odr.Read())
            {
                string custAcNo = odr["cust_ac_no"].ToString();

                OracleCommand cmd1 = new OracleCommand(StatementBalance.getQuery(fromDate, toDate, custAcNo));
                cmd1.Connection = con;
                OracleDataReader dr = cmd1.ExecuteReader();
                while (dr.Read())
                {
                    string customerName = dr["ac_desc"].ToString();
                    string ccy = dr["ccy"].ToString();
                    string description = dr["description"].ToString();
                    decimal openingBal = Convert.ToDecimal(dr["acy_opening_balance"].ToString());
                    decimal avlBal = Convert.ToDecimal(dr["closing_balance"].ToString());
                    string branchName = dr["branch_name"].ToString();
                    string branchAddr1 = dr["branch_addr1"].ToString();
                    string address2 = dr["address2"].ToString();
                    decimal drTurnOver = Convert.ToDecimal(dr["dr_tot"].ToString());
                    decimal crTurnOver = Convert.ToDecimal(dr["cr_tot"].ToString());

                    dt.Rows.Add(customerName, ccy, description, openingBal, avlBal, custAcNo, branchName, branchAddr1, address2, drTurnOver, crTurnOver);
                }
            }
            ds.Tables.Add(dt);

            return ds;
        }

        private void GenerateAndSaveStatementsForAllAccounts(string custId, string fromDate, string toDate, OracleConnection mainConn, JLogger logger, decimal runningBalance)
        {
            logger.Info("About to get account lists..");
            Oracle.DataAccess.Client.OracleConnection con = mainConn;
            List<String> custAccountList = new List<string>();
            try
            {
                OracleCommand getAccountsCmd = new OracleCommand("select cust_ac_no from sttm_cust_account where cust_no = '" + custId.Trim() + "' AND RECORD_STAT = 'O' AND ac_stat_dormant = 'N' AND ac_stat_frozen = 'N' AND AUTH_STAT = 'A'");
                getAccountsCmd.Connection = con;
                OracleDataReader odr = getAccountsCmd.ExecuteReader();
                while (odr.Read())
                {
                    String custAcNo = (string)odr["cust_ac_no"];
                    custAccountList.Add(custAcNo);
                }
            }
            catch (Exception ex) { logger.Info(ex.Message, ex); }

            try
            {
                logger.Info("Number of accounts found is " + custAccountList.Count);
                logger.Info("about to add each account to the datasets");
                //decimal runningBalance = 0;
                string path = @"C:\eaes\" + custId + "\\";
                int cnt = 2;
                foreach (String accountNum in custAccountList)
                {
                    cnt++;
                    DataSet ds2 = new DataSet();
                    decimal runBal;
                    ds2 = GetAccountRunningBalance(accountNum, fromDate, toDate, logger, out runBal, mainConn);
                    DataSet dscnt = new DataSet();
                    dscnt = GetAccountTransactionBetweenDates(accountNum, fromDate, toDate, runBal, mainConn, logger);
                    //DataSet ds3 = new DataSet();
                    //ds3 = GetChannelCountsAndPercentages();
                    if (dscnt.Tables[0].Rows.Count > 0)
                    {
                        String v_mimetype;
                        String v_encoding;
                        String v_filename_extension;
                        String[] v_streamids;
                        Microsoft.Reporting.WinForms.Warning[] warnings;

                        ReportViewer reportViewer1 = new ReportViewer();
                        reportViewer1.LocalReport.ReportPath = @"C:\eaes\config\Report2.rdlc";

                        reportViewer1.RefreshReport();

                        ReportDataSource rds3 = new ReportDataSource("DataSet1", dscnt.Tables[0]);
                        reportViewer1.LocalReport.DataSources.Add(rds3);

                        ReportDataSource rds2 = new ReportDataSource("DataSet2", ds2.Tables[0]);
                        reportViewer1.LocalReport.DataSources.Add(rds2);

                        //ReportDataSource rds1 = new ReportDataSource("DataSet2", ds3.Tables[0]);
                        //reportViewer1.LocalReport.DataSources.Add(rds1);

                        DirectoryInfo dI = new DirectoryInfo(path);
                        if (!dI.Exists) { dI.Create(); }

                        byte[] byteViewer = reportViewer1.LocalReport.Render("PDF", null, out v_mimetype, out v_encoding, out v_filename_extension, out v_streamids, out warnings);

                        //disposing of the reportviewer here...
                        reportViewer1.Dispose();

                        FileStream newFile = new FileStream(path + accountNum + cnt + ".pdf", FileMode.Create);
                        newFile.Write(byteViewer, 0, byteViewer.Length);
                        newFile.Close();
                        newFile.Dispose();
                    }
                }
            }
            catch (Exception ex) { logger.Info(ex.Message, ex); }
        }

        private DataSet GetAccountRunningBalance(string accountNumber, string fromDate, string toDate, JLogger logger, out decimal RunningBalance, OracleConnection mainConn)
        {
            RunningBalance = 0;

            Oracle.DataAccess.Client.OracleConnection con = mainConn;
            //con.Open();
            string alternateQuery = "SELECT B.CUST_AC_NO,B.ACY_AVL_BAL,B.AC_DESC,C.CCY_NAME,D.DESCRIPTION FROM STTM_CUST_ACCOUNT B, CYTM_CCY_DEFN C, STTM_ACCOUNT_CLASS D WHERE C.CCY_CODE = B.CCY AND D.ACCOUNT_CLASS = B.ACCOUNT_CLASS AND B.CUST_AC_NO = '" + accountNumber + "'";
            OracleCommand cmd = new OracleCommand("select A.ACCOUNT,A.ACY_OPENING_BAL,B.AC_DESC,C.CCY_NAME,D.DESCRIPTION from STVW_BALANCE_HISTORY A, STTM_CUST_ACCOUNT B,CYTM_CCY_DEFN C,STTM_ACCOUNT_CLASS D where A.ACCOUNT = B.CUST_AC_NO AND C.CCY_CODE = B.CCY AND D.ACCOUNT_CLASS = B.ACCOUNT_CLASS AND A.account = '" + accountNumber + "' and BKG_DATE between '" + fromDate + "' and '" + toDate + "' order by A.BKG_DATE asc");

            //using (con)
            //{
            cmd.Connection = con;
            logger.Info("About to execute the 1st query....");
            OracleDataReader dr = cmd.ExecuteReader();
            

            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[5]
                    {
                        new DataColumn("AC_NO", typeof(string)),
                        new DataColumn("CUSTOMER_NAME", typeof(string)) ,
                        new DataColumn("PRODUCT_NAME", typeof(string)),
                        new DataColumn("CURRENCY", typeof(string)),
                        new DataColumn("OPENING_BALANCE", typeof(decimal))
                    });
            logger.Info("First query row size : " + dr.FieldCount);
            int counter = 0;
            DataSet ds1 = new DataSet();
            while (dr.Read())
            {
                string acDesc = dr["AC_DESC"].ToString();
                decimal openingBalance = (decimal)dr["ACY_OPENING_BAL"];
                string acNo = dr["ACCOUNT"].ToString();
                string ccy = dr["CCY_NAME"].ToString();
                string ProductName = dr["DESCRIPTION"].ToString();

                //this is a date range calc for the opening balance....
                openingBalance = getOpeningBal(accountNumber, fromDate, mainConn, logger);

                RunningBalance = openingBalance;

                dt.Rows.Add(acNo, acDesc, ProductName, ccy, openingBalance);
                counter++;
                //lets break it since we have gotten the first data.
                break;
            }

            //we execute this query when we don't get any result from the first query.

            if (counter < 1)
            {
                logger.Info("About to execute the second query");
                OracleCommand cmd1 = new OracleCommand(alternateQuery);
                cmd1.Connection = con;
                try
                {
                    OracleDataReader reader = cmd1.ExecuteReader();
                    logger.Info("Finished executing the second query, result set is : " + reader.FetchSize);
                    while (reader.Read())
                    {
                        logger.Info("Second query returned result....");
                        string acDesc = reader["AC_DESC"].ToString();
                        decimal openingBalance = (decimal)reader["ACY_AVL_BAL"];
                        string acNo = reader["CUST_AC_NO"].ToString();
                        string ccy = reader["CCY_NAME"].ToString();
                        string ProductName = reader["DESCRIPTION"].ToString();

                        //this is a date range calc for the opening balance....
                        openingBalance = getOpeningBal(accountNumber, fromDate, mainConn, logger);

                        RunningBalance = openingBalance;

                        dt.Rows.Add(acNo, acDesc, ProductName, ccy, openingBalance);
                    }
                }
                catch (Exception ex)
                {
                    logger.Info(ex.Message, ex);
                }

            }

            ds1.Tables.Add(dt);

            return (ds1);
            
        }

        public decimal getOpeningBal(string acNo, string fromDate, OracleConnection mainConn, JLogger logger)
        {
            decimal bal = 0;
            string query = "SELECT SUM(DECODE(drcr_ind,'C',lcy_amount,-lcy_amount)) as open_bal FROM acvw_all_ac_entries WHERE ac_no = '" + acNo + "' AND trn_dt  < '" + fromDate + "' GROUP BY ac_no ORDER BY ac_no";

            logger.Info("About to get opening balance");
            logger.Info("query is => " + query);
            Oracle.DataAccess.Client.OracleConnection con = mainConn;
            List<String> custAccountList = new List<string>();
            try
            {
                OracleCommand getAccountsCmd = new OracleCommand(query);
                getAccountsCmd.Connection = con;
                OracleDataReader odr = getAccountsCmd.ExecuteReader();
                while (odr.Read())
                {
                    bal = (decimal)odr["open_bal"];
                }
            }
            catch (Exception ex) { logger.Info(ex.Message, ex); }

            return bal;
        }
    }



}
