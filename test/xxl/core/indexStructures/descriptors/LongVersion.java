package xxl.core.indexStructures.descriptors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import xxl.core.indexStructures.MVBTree;
import xxl.core.indexStructures.MVBTree.Version;
import xxl.core.indexStructures.mvbts.SimpleLoadMVBTree;
import xxl.core.io.converters.Converter;
import xxl.core.io.converters.LongConverter;
import xxl.core.io.converters.MeasuredConverter;


/**
 * This class is implements interface @see {@link Version} and is used in the test class @see {@link SimpleLoadMVBTree}.
 * Assumption: time stamps are of type long. 
 */
public class LongVersion implements MVBTree.Version{
	/**
	 * 
	 */
	public long version;
	/**
	 * 
	 */
	public static final Converter<LongVersion> VERSION_CONVERTER =	new Converter<LongVersion>(){
		@Override
		public LongVersion read(DataInput dataInput, LongVersion object)
				throws IOException {
			long version = LongConverter.DEFAULT_INSTANCE.readLong(dataInput);
			return new LongVersion(version);
		}
		@Override
		public void write(DataOutput dataOutput, LongVersion object)
				throws IOException {
			LongConverter.DEFAULT_INSTANCE.writeLong(dataOutput, ((LongVersion)object).version);
		}
	};
	/**
	 * 
	 */
	public static final MeasuredConverter<LongVersion> VERSION_MEASURED_CONVERTER = new MeasuredConverter<LongVersion>() {
		@Override
		public int getMaxObjectSize() {
			return LongConverter.SIZE;
		}
		@Override
		public LongVersion read(DataInput dataInput, LongVersion object)
				throws IOException {
			return VERSION_CONVERTER.read(dataInput) ;
		}
		@Override
		public void write(DataOutput dataOutput, LongVersion object)
				throws IOException {
			VERSION_CONVERTER.write(dataOutput, object);
		}
	};
	/**
	 * 
	 * @param version
	 */
	public LongVersion(long version){
		this.version = version;
	}
	/**
	 * 
	 * @return
	 */
	public Long getTimeStamp(){
		return this.version;
	}
	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Object o) {
		LongVersion v = (LongVersion)o;
		return (this.version == v.version) ? 0 : ( (this.version > v.version ) ? 1: -1) ;
	}
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#clone()
	 */
	public Object clone(){
		return new LongVersion(this.version);
	}
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return ((int)version * 157) & 0x7fffffff;
	}
	/**
	 * 
	 */
	public String toString(){
		return "Version: " + this.version; 
	}
}
