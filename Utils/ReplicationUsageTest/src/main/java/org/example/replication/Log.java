package org.example.replication;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/** 화면과 실행 로그 파일에 같은 줄을 남긴다. 형식: "2026-11-25 11:10:59,123 INFO    메시지" */
public final class Log {
	private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
	private static Path file;

	private Log() {
	}

	/** 이후 로그를 이 파일에도 덧붙인다. */
	public static synchronized void toFile(Path path) {
		file = path;
	}

	public static void info(String format, Object... args) {
		write("INFO", String.format(format, args));
	}

	public static void warning(String format, Object... args) {
		write("WARNING", String.format(format, args));
	}

	public static void error(String format, Object... args) {
		write("ERROR", String.format(format, args));
	}

	/** 오류와 스택 트레이스를 함께 남긴다(예상 못 한 오류용). */
	public static void exception(Throwable e, String format, Object... args) {
		var trace = new StringWriter();
		e.printStackTrace(new PrintWriter(trace));
		write("ERROR", String.format(format, args) + System.lineSeparator() + trace.toString().stripTrailing());
	}

	private static synchronized void write(String level, String message) {
		String line = String.format("%s %-7s %s", LocalDateTime.now().format(TIME), level, message);
		System.out.println(line);
		if (file != null) {
			try {
				Files.writeString(file, line + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE,
						StandardOpenOption.APPEND);
			} catch (IOException e) {
				System.err.println("로그 파일 쓰기 실패: " + file + " (" + e.getMessage() + ")");
			}
		}
	}
}
