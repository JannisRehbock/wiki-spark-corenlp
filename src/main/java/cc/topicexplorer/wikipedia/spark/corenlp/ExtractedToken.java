package cc.topicexplorer.wikipedia.spark.corenlp;

import java.io.Serializable;

public class ExtractedToken implements Serializable {

	private static final long serialVersionUID = -2215905648059993561L;
	public String token;
	public Integer beginPosition;
	public Integer endPosition;
	public String lemma;
	public String pos;
	public Long pageId;
	public Integer sentenceNo;
	public Integer tokenNo;
	
	public Integer getTokenNo() {
		return tokenNo;
	}
	public void setTokenNo(Integer tokenNo) {
		this.tokenNo = tokenNo;
	}
	public Long getPageId() {
		return pageId;
	}
	public void setPageId(Long pageId) {
		this.pageId = pageId;
	}
	public Integer getSentenceNo() {
		return sentenceNo;
	}
	public void setSentenceNo(Integer sentenceNo) {
		this.sentenceNo = sentenceNo;
	}
	public String getToken() {
		return token;
	}
	public void setToken(String token) {
		this.token = token;
	}
	public Integer getBeginPosition() {
		return beginPosition;
	}
	public void setBeginPosition(Integer beginPosition) {
		this.beginPosition = beginPosition;
	}
	public Integer getEndPosition() {
		return endPosition;
	}
	public void setEndPosition(Integer endPosition) {
		this.endPosition = endPosition;
	}
	public String getLemma() {
		return lemma;
	}
	public void setLemma(String lemma) {
		this.lemma = lemma;
	}
	public String getPos() {
		return pos;
	}
	public void setPos(String pos) {
		this.pos = pos;
	}

}
