package org.hl7.tinkar.lombok.dto.json;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *  Used to indicate which static method on a class shall be used as the
 *  JsonChronologyUnmarshaler.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface JsonSemanticVersionUnmarshaler {

}