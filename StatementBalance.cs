using System;
using System.Collections.Generic;
using System.Text;

namespace GenerateStatementLib
{
    public class StatementBalance
    {
        public static string getQuery(string fromDate, String toDate, string acNo)
        {
            string query = @"SELECT '{0}' statement_start,
  '{1}' statement_end,
  NVL(DR_CNT,0) DR_CNT,
  NVL(CR_CNT,0) CR_CNT,
  NVL(DR_TOT,0) DR_TOT,
  NVL(CR_TOT,0) CR_TOT,
  NVL(opening_balance,0) acy_opening_balance,
  NVL(closing_balance,0) closing_balance,
  0 statement_status,
  a.ac_desc,
  a.ccy,
  a.acy_avl_bal,
  a.cust_no,
  (select description from sttm_account_class where account_class = a.account_class) description,
  a.branch_code,
  (select branch_name from sttm_branch where branch_code = a.branch_code) branch_name,
  (select branch_addr1 from sttm_branch where branch_code = a.branch_code) branch_addr1,
  a.address2
FROM STTM_CUST_ACCOUNT a
JOIN lmtms_liab h
ON a.cust_no=h.liab_id
LEFT JOIN
  (SELECT ac_no,
    SUM(DECODE(cod_drcr,'D',1,0)) DR_CNT,
    SUM(DECODE(cod_drcr,'C',1,0)) CR_CNT,
    SUM(DECODE(cod_drcr,'D',amt_txn,0)) DR_TOT,
    SUM(DECODE(cod_drcr,'C',amt_txn,0)) CR_TOT
  FROM
    (SELECT ac_no,
      ROUND(DECODE(ac_ccy,'GHS',lcy_amount,DECODE(trim(fcy_amount),NULL,lcy_amount/exch_rate,'',lcy_amount/exch_rate,fcy_amount)),2) amt_txn,
      drcr_ind cod_drcr
    FROM acvw_all_ac_entries
    WHERE trn_dt BETWEEN '{0}' AND '{1}'
    AND event <> 'REVL'
    )
  GROUP BY ac_no
  ) i
ON a.cust_ac_no=i.ac_no
LEFT JOIN
  (SELECT ac_no,
    DECODE(SUM(DECODE(cod_drcr,'D',-1,1) * amt_txn),NULL,0,SUM(DECODE(cod_drcr,'D',-1,1) * amt_txn)) opening_balance
  FROM
    (SELECT ac_no,
      ROUND(DECODE(ac_ccy,'GHS',lcy_amount,DECODE(trim(fcy_amount),NULL,lcy_amount/exch_rate,'',lcy_amount/exch_rate,fcy_amount)),2) amt_txn,
      drcr_ind cod_drcr
    FROM acvw_all_ac_entries
    WHERE trn_dt < '{0}'
    AND event   <> 'REVL'
    )
  GROUP BY ac_no
  ) j
ON a.cust_ac_no=j.ac_no
LEFT JOIN
  (SELECT ac_no,
    DECODE(SUM(DECODE(cod_drcr,'D',-1,1) * amt_txn),NULL,0,SUM(DECODE(cod_drcr,'D',-1,1) * amt_txn)) closing_balance
  FROM
    (SELECT ac_no,
      ROUND(DECODE(ac_ccy,'GHS',lcy_amount,DECODE(trim(fcy_amount),NULL,lcy_amount/exch_rate,'',lcy_amount/exch_rate,fcy_amount)),2) amt_txn,
      drcr_ind cod_drcr
    FROM acvw_all_ac_entries
    WHERE trn_dt <= '{1}'
    AND event    <> 'REVL'
    )
  GROUP BY ac_no
  ) x
ON a.cust_ac_no   =x.ac_no
WHERE a.cust_ac_no='{2}'
AND a.auth_stat   ='A'
AND h.auth_stat   ='A'
AND a.record_stat = 'O'";
            return string.Format(query, fromDate, toDate, acNo);
        }
    }
}
