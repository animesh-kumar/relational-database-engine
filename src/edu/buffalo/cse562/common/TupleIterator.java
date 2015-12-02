package edu.buffalo.cse562.common;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Consumer;

import edu.buffalo.cse562.schema.Tuple;

public class TupleIterator implements Iterator<Tuple>{

	private List<Tuple> tuples;
	private ListIterator<Tuple> listIterator;
	
	public TupleIterator(List<Tuple> tupleList) {
		    tuples = tupleList;
		    listIterator = tuples.listIterator();
	}
	
	@Override
	public boolean hasNext() {
		return listIterator.hasNext()?true:false;
	}

	@Override
	public Tuple next() {
		if(hasNext()){
			return listIterator.next();
		}
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void forEachRemaining(Consumer<? super Tuple> action) {
		// TODO Auto-generated method stub
		
	}

	

}
