package com.github.rtempleton.operators.trie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;


public class LongestSequenceTrie implements Trie {


	private static final long serialVersionUID = 1L;
	private final Map<Character, LongestSequenceTrie> trie;
	private ArrayList<Object> values = new ArrayList<Object>();
	
	public LongestSequenceTrie() {
		this.trie = new Hashtable<Character, LongestSequenceTrie>();
	}
	
	@Override
	public void buildTrie(String key, Object token) {
		if(key!=null && !key.trim().isEmpty()){
			char[] c = key.toCharArray();
			buildTrie(c, token);
		}
	}
	
	private void buildTrie(char[] c, Object token){
		if(c.length == 0){
			this.values.add(token);
		}else{	
			LongestSequenceTrie child;
			if(this.trie.get(c[0])==null){
				child = new LongestSequenceTrie();
				this.trie.put(c[0], child);
			}else{
				child = this.trie.get(c[0]);
			}
			
			char[] subc = new char[c.length-1];
			for(int j=1;j<c.length;j++){
				subc[j-1]=c[j];
			}
			child.buildTrie(subc, token);
		}
		
	}

	@Override
	public void walkTrie(String val, List<Object> results) {
		if(val!=null){
			char[] c = val.toCharArray();
			if(c.length>1)
				walkTrie(c, results);
		}
	}
	
	private void walkTrie(char[] c, List<Object> results){
		//set the result to the current node as long as the value isn't null
		//this should only return one (longest) match to clear the previous longest match when we find a new longer one
		if(values.size()!=0){
			results.clear();
			results.addAll(values);
		}
		
		if(c.length>0){
			LongestSequenceTrie t = trie.get(c[0]);
			if(t!=null){
				t.walkTrie(Arrays.copyOfRange(c, 1, c.length), results);
			}
		}

	}

}
