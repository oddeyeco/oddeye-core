/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import java.io.Serializable;

/**
 *
 * @author vahan
 */
abstract public class ErrorStateBase implements Serializable, Comparable<ErrorStateBase>{
    private static final long serialVersionUID = 465895478L;
    
    @Override
    public int compareTo(ErrorStateBase o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
