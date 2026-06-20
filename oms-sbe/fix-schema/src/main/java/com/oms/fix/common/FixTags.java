package com.oms.fix.common;

/** FIX 4.4 tag number constants and MsgType values. */
public final class FixTags {
    private FixTags() {}

    // Tag numbers
    public static final int ACCOUNT         = 1;
    public static final int AVG_PX          = 6;
    public static final int CL_ORD_ID       = 11;
    public static final int CUM_QTY         = 14;
    public static final int EXEC_ID         = 17;
    public static final int MSG_TYPE        = 35;
    public static final int ORDER_QTY       = 38;
    public static final int ORD_STATUS      = 39;
    public static final int ORIG_CL_ORD_ID  = 41;
    public static final int PRICE           = 44;
    public static final int SIDE            = 54;
    public static final int SYMBOL          = 55;
    public static final int TEXT            = 58;
    public static final int TRANSACT_TIME   = 60;
    public static final int ORDER_ID        = 37;
    public static final int EXEC_TYPE       = 150;
    public static final int LEAVES_QTY      = 151;

    // MsgType values (tag 35)
    public static final String NEW_ORDER_SINGLE        = "D";
    public static final String ORDER_CANCEL_REQUEST    = "F";
    public static final String ORDER_CANCEL_REPLACE    = "G";
    public static final String EXECUTION_REPORT        = "8";
    public static final String ORDER_CANCEL_REJECT     = "9";
    public static final String BUSINESS_MESSAGE_REJECT = "j";

    // ExecType values (tag 150)
    public static final char EXEC_TYPE_NEW       = '0';
    public static final char EXEC_TYPE_TRADE     = 'F';
    public static final char EXEC_TYPE_CANCELED  = '4';
    public static final char EXEC_TYPE_REPLACE   = '5';
    public static final char EXEC_TYPE_REJECTED  = '8';

    // OrdStatus values (tag 39)
    public static final char ORD_STATUS_NEW              = '0';
    public static final char ORD_STATUS_PARTIALLY_FILLED = '1';
    public static final char ORD_STATUS_FILLED           = '2';
    public static final char ORD_STATUS_CANCELED         = '4';
    public static final char ORD_STATUS_REJECTED         = '8';
}
