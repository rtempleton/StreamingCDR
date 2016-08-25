package com.github.rtempleton.cdr_storm.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class PhoenixJDBCConn {
	
	private LoadingCache<String, Integer> cache = CacheBuilder.newBuilder()
			.maximumSize(10)
			.expireAfterAccess(24, TimeUnit.HOURS)
			.build(new CacheLoader<String, Integer>(){
				@Override
				public Integer load(String s){
					return execute(s);
				}
			});
	
	public Integer execute(String key){
		String[] keys = key.split("-");
		String query = "select DATE_ID from cdrdwh.date_dim where YEAR_VAL = " + keys[0] + " and DAY_OF_YEAR = " + keys[1];
		Integer i =0;
		try{
			Connection con = DriverManager.getConnection("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure");
			PreparedStatement stmt = con.prepareStatement(query);
			ResultSet rset = stmt.executeQuery();
			while (rset.next()){
				i=rset.getInt("DATE_ID");
			}
			stmt.close();
			con.close();
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
		return i;
	}
	
	public int getDateId(String key){
		try{
			return cache.get(key);
		}catch(Exception e){
			return -1;
		}
	}
}
