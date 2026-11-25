package org.example.replication.cases;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TC19 룰별 조건 다른 다중 대상: 원본 하나에 조건이 다른 룰 3개를 걸고, 객체마다 맞는 룰의 대상으로만 가는지 본다.
 *
 * | 룰 | Priority | Filter | 삭제 마커 복제 | 대상 |
 * |---|---|---|---|---|
 * | 1 | 1 | Prefix a/ | 켬 | dst |
 * | 2 | 2 | Prefix b/ | 끔 | dst2 |
 * | 3 | 3 | 빈 Filter(전체) | 끔 | dst3 |
 *
 * a/·b/·c/ 키를 PUT 한 뒤 접두어마다 절반을 버전 없이 지워 삭제 마커를 만든다. 기대값은 다음과 같다.
 *
 * - dst: a/ 버전 전부 + a/ 삭제 마커
 * - dst2: b/ 버전 전부, 삭제 마커 없음
 * - dst3: a/·b/·c/ 버전 전부, 삭제 마커 없음(a/ 는 룰 1 에 맞아도 룰 3 의 삭제 마커 설정을 따른다)
 *
 * 대상 버킷이 서로 다르면 객체가 맞는 룰 모두에 복제된다(Priority 는 대상이 같은 룰끼리만 고른다).
 * 룰 하나만 적용하고 멈추거나, 다른 룰의 삭제 마커 설정을 가져다 쓰면 실패한다.
 */
public class Tc19MultiRuleMixed extends ReplicationCase {
	static final List<String> PREFIXES = List.of("a/", "b/", "c/");

	/** 대상 순서대로 (Filter, 삭제 마커 복제, 복제되는 접두어. null 이면 전부). */
	record Rule(RuleSpec spec, String prefix) {
	}

	static final List<Rule> RULES = List.of(
			new Rule(new RuleSpec(prefixFilter("a/"), true), "a/"),
			new Rule(new RuleSpec(prefixFilter("b/"), false), "b/"),
			new Rule(new RuleSpec(emptyFilter(), false), null));

	public Tc19MultiRuleMixed() {
		super("TC19", "룰별 조건 다른 다중 대상(룰 3개)", "a/·b/·c/ PUT 후 삭제(마커 생성)");
		destinations = RULES.size();
		hasNegative = true;
	}

	@Override
	protected RuleSpec ruleSpec(int index) {
		return RULES.get(index).spec();
	}

	@Override
	protected boolean replicatesTo(int index, String key, Map<String, String> tags) {
		String prefix = RULES.get(index).prefix();
		return prefix == null || key.startsWith(prefix);
	}

	@Override
	protected void operate(List<String> detail) {
		List<String> parts = new ArrayList<>();
		for (String prefix : PREFIXES)
			Tc06DeleteMarkerOff.putThenDelete(this, parts, prefix);
		detail.add(String.join(", ", parts));
	}
}
