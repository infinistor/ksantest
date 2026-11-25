package org.example.replication;

import java.util.Map;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/** 포털 응답(PascalCase·camelCase 섞임)을 읽는 보조 함수. */
final class Json {
	private Json() {
	}

	/** 대소문자 구분 없이 값을 꺼낸다. 없거나 null 이면 null. */
	static JsonElement getCi(JsonObject d, String key) {
		for (Map.Entry<String, JsonElement> e : d.entrySet())
			if (e.getKey().equalsIgnoreCase(key))
				return e.getValue() == null || e.getValue().isJsonNull() ? null : e.getValue();
		return null;
	}

	static String toStr(JsonElement e) {
		return e == null ? "" : e.isJsonPrimitive() ? e.getAsString() : e.toString();
	}

	static long toLong(JsonElement e) {
		if (e == null || !e.isJsonPrimitive())
			return 0;
		var p = e.getAsJsonPrimitive();
		if (p.isBoolean())
			return p.getAsBoolean() ? 1 : 0;
		String s = p.getAsString().strip();
		if (s.isEmpty())
			return 0;
		try {
			return p.isNumber() ? p.getAsNumber().longValue() : Long.parseLong(s);
		} catch (NumberFormatException ex) {
			throw new CaseFailed("포털 응답 값이 정수가 아님: " + s);
		}
	}

	static int toInt(JsonElement e) {
		return (int) toLong(e);
	}

	/** 짧게 잘라 메시지에 넣는다. */
	static String shorten(Object o, int max) {
		String s = String.valueOf(o);
		return s.length() <= max ? s : s.substring(0, max);
	}
}
