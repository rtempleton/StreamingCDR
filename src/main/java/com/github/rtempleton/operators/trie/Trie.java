package com.github.rtempleton.operators.trie;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.tuple.Tuple;

public interface Trie extends Serializable {
	
	public void buildTrie(String key, Object token);
	
	public void walkTrie(String val, List<Object> results);

}
