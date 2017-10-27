/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package co.oddeye.core;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author vahan
 */
public class OddeyeMessage extends MimeMessage {

    public OddeyeMessage(Session session) {
        super(session);
    }

    @Override
    protected void updateMessageID() throws MessagingException {
	super.updateMessageID();               
        StringBuilder s = new StringBuilder();
        // Unique string is <hashcode>.<id>.<currentTime>.JavaMail.<suffix>
        
        s.append(s.hashCode()).append('.').append(System.currentTimeMillis()).append('.').append(session.getProperty("mail.user"));
//
//        setHeader("Message-ID", "<" + s.toString() + ">");
	setHeader("Message-ID", "<"+s.toString()+">");

    }
}
