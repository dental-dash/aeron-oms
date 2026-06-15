package com.oms.fix.common;

import com.epam.deltix.dfp.Decimal64Utils;
import com.oms.fix.sbe.Decimal64Decoder;

public class Decimal64Util {

    public static long fromFixedPoint(final Decimal64Decoder decimal64Decoder) {
        return Decimal64Utils.fromFixedPoint(decimal64Decoder.mantissa(), decimal64Decoder.exponent());
    }
}
