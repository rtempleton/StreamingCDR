package com.github.rtempleton.cdr_storm.utils;

import java.sql.SQLException;

import org.junit.Ignore;
import org.junit.Test;

import com.github.rtempleton.poncho.test.AbstractTestCase;

public class TestPhoenixJDBC extends AbstractTestCase {
	
	@Ignore
	public void test() throws SQLException{
		new PhoenixJDBCConn().execute("select date_id, day_of_year, year_val from cdrdwh.date_dim");
	}
	
	@Test
	public void test2(){
		PhoenixJDBCConn c = new PhoenixJDBCConn();
		System.out.println(c.getDateId("2011-1"));
		System.out.println(c.getDateId("2011-1"));
		System.out.println(c.getDateId("2011-2"));
	}
	

}
