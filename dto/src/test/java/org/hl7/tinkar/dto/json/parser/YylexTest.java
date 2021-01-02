/*
 * Copyright 2020 kec.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hl7.tinkar.dto.json.parser;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;

import org.hl7.tinkar.dto.json.parser.ParseException;
import org.hl7.tinkar.dto.json.parser.Yylex;
import org.hl7.tinkar.dto.json.parser.Yytoken;
import org.junit.jupiter.api.Test;

public class YylexTest {

    @Test
    public void testYylex() throws Exception {
        String s = "\"\\/\"";
        System.out.println(s);
        StringReader in = new StringReader(s);
        Yylex lexer = new Yylex(in);
        Yytoken token = lexer.yylex();
        assertEquals(Yytoken.Type.TYPE_VALUE, token.type);
        assertEquals("/", token.value);

        s = "\"abc\\/\\r\\b\\n\\t\\f\\\\\"";
        System.out.println(s);
        in = new StringReader(s);
        lexer = new Yylex(in);
        token = lexer.yylex();
        assertEquals(Yytoken.Type.TYPE_VALUE, token.type);
        assertEquals("abc/\r\b\n\t\f\\", token.value);

        s = "[\t \n\r\n{ \t \t\n\r}";
        System.out.println(s);
        in = new StringReader(s);
        lexer = new Yylex(in);
        token = lexer.yylex();
        assertEquals(Yytoken.Type.TYPE_LEFT_SQUARE, token.type);
        token = lexer.yylex();
        assertEquals(Yytoken.Type.TYPE_LEFT_BRACE, token.type);
        token = lexer.yylex();
        assertEquals(Yytoken.Type.TYPE_RIGHT_BRACE, token.type);

        s = "\b\f{";
        System.out.println(s);
        in = new StringReader(s);
        lexer = new Yylex(in);
        ParseException err = null;
        try {
            lexer.yylex();
        } catch (ParseException e) {
            err = e;
            System.out.println("error:" + err);
            assertEquals(ParseException.ErrorType.UNEXPECTED_CHAR, e.getErrorType());
            assertEquals(0, e.getPosition());
            assertEquals("\b", e.getUnexpectedObjectString());
        }
        assertNotNull(err);

        s = "{a : b}";
        System.out.println(s);
        in = new StringReader(s);
        lexer = new Yylex(in);
        err = null;
        try {
            lexer.yylex();
            lexer.yylex();
        } catch (ParseException e) {
            err = e;
            System.out.println("error:" + err);
            assertEquals(ParseException.ErrorType.UNEXPECTED_CHAR, e.getErrorType());
            assertEquals("a", e.getUnexpectedObjectString());
            assertEquals(1, e.getPosition());
        }
        assertNotNull(err);
    }

}