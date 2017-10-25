/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

/**
 *
 * @author vahan
 */
public class InvalidKeyException extends Exception {

    private static final long serialVersionUID = 465895478L;

    InvalidKeyException(String string) {
        super(string);
    }

}
