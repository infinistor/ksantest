package org.example.replication;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.example.replication.cases.Cases;
import org.example.replication.cases.ReplicationCase;

import software.amazon.awssdk.services.s3.S3Client;

/**
 * 복제 usage 테스트 실행 진입점.
 *
 * <pre>
 * java -jar replication-usage-test.jar                      # 같은 시스템(local) + 다른 리전(remote) 전부
 * java -jar replication-usage-test.jar --target local       # 같은 시스템만
 * java -jar replication-usage-test.jar --target remote      # 포털에 등록된 다른 리전만
 * java -jar replication-usage-test.jar --region kr-ksan-1   # remote 를 이 리전으로만 (여러 번 줄 수 있음)
 * java -jar replication-usage-test.jar --tc TC01            # 지정 케이스만 (여러 번 줄 수 있음)
 * java -jar replication-usage-test.jar --list               # 케이스 목록
 * java -jar replication-usage-test.jar --check              # 설정·포털·리전·키 점검만
 * </pre>
 *
 * 원본 S3 주소는 포털의 기본 리전, 원격 대상은 포털에 등록된 나머지 리전에서 가져온다.
 */
public final class Main {
	static final int EXIT_OK = 0;
	static final int EXIT_FAIL = 1;
	static final int EXIT_CONFIG = 2;
	static final String SUMMARY_HEADER = "run_time,case,result,failed_step,replication_sec,"
			+ "src_bucket,src_count,src_size,dst_bucket,dst_count,dst_size,message";

	private Main() {
	}

	/** 명령행 옵션. */
	static final class Args {
		Path config;
		String target = "all";
		List<String> regions = new ArrayList<>();
		List<String> tcs = new ArrayList<>();
		boolean list;
		boolean check;
		Integer cleanupDays;
	}

	static final String USAGE = """
			사용법: java -jar replication-usage-test.jar [옵션]
			  --config FILE       설정 파일 (기본: jar 옆 config.ini)
			  --target T          복제 대상 all·local·remote (기본 all)
			  --region NAME       remote 대상 리전 이름 (여러 번 줄 수 있음)
			  --tc ID             실행할 케이스 ID, 예: TC01 (여러 번 줄 수 있음)
			  --list              케이스 목록만 출력
			  --check             설정·포털·리전·키 점검만
			  --cleanup-days N    실행 전에 이름의 시각이 N일보다 오래된 시험 버킷(rtest-*)을 지운다 (기본 끔)
			""";

	static Args parseArgs(String[] argv, Path baseDir) {
		var a = new Args();
		a.config = baseDir.resolve("config.ini");
		for (int i = 0; i < argv.length; i++) {
			String arg = argv[i];
			String value = null;
			int eq = arg.indexOf('=');
			if (arg.startsWith("--") && eq > 0) {
				value = arg.substring(eq + 1);
				arg = arg.substring(0, eq);
			}
			switch (arg) {
				case "-h", "--help" -> {
					System.out.print(USAGE);
					System.exit(EXIT_OK);
				}
				case "--list" -> a.list = true;
				case "--check" -> a.check = true;
				case "--config", "--target", "--region", "--tc", "--cleanup-days" -> {
					if (value == null) {
						if (i + 1 >= argv.length)
							throw new ConfigError(arg + " 에 값이 필요함");
						value = argv[++i];
					}
					switch (arg) {
						case "--config" -> a.config = Path.of(value);
						case "--target" -> {
							if (!List.of("all", "local", "remote").contains(value))
								throw new ConfigError("--target 은 all·local·remote 중 하나: " + value);
							a.target = value;
						}
						case "--region" -> a.regions.add(value);
						case "--tc" -> a.tcs.add(value);
						default -> {
							try {
								a.cleanupDays = Integer.parseInt(value);
							} catch (NumberFormatException e) {
								throw new ConfigError("--cleanup-days 는 정수: " + value);
							}
							if (a.cleanupDays < 0)
								throw new ConfigError("--cleanup-days 는 0 이상");
						}
					}
				}
				default -> throw new ConfigError("알 수 없는 옵션: " + arg + "\n" + USAGE);
			}
		}
		return a;
	}

	/** jar(또는 클래스 폴더)가 있는 폴더. 설정·결과 파일의 기준이다. */
	static Path baseDir() {
		try {
			Path p = Path.of(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI());
			return Files.isDirectory(p) ? p : p.getParent();
		} catch (Exception e) {
			return Path.of("").toAbsolutePath();
		}
	}

	/** 원본 리전, 원본 S3 클라이언트, 대상 목록. */
	record Built(PortalRegion home, S3Client s3, List<Target> targets) {
	}

	static Built buildTargets(Settings settings, PortalClient portal, String which, List<String> regionsWanted) {
		PortalRegion home = portal.defaultRegion();
		S3Client s3 = Common.makeS3Client(settings.sourceEndpoint(home)[0], settings.accessKey, settings.secretKey);
		List<Target> targets = new ArrayList<>();
		if (which.equals("all") || which.equals("local"))
			targets.add(Target.local(s3));
		if (which.equals("all") || which.equals("remote")) {
			List<PortalRegion> others = portal.regions().stream().filter(r -> !r.name().equals(home.name())).toList();
			if (!regionsWanted.isEmpty()) {
				Set<String> names = others.stream().map(PortalRegion::name).collect(Collectors.toCollection(TreeSet::new));
				Set<String> unknown = new TreeSet<>(regionsWanted);
				unknown.removeAll(names);
				if (!unknown.isEmpty())
					throw new ConfigError("포털에 없는(또는 원본) 리전: " + String.join(", ", unknown) + " (다른 리전: "
							+ (names.isEmpty() ? "없음" : String.join(", ", names)) + ")");
				others = others.stream().filter(r -> regionsWanted.contains(r.name())).toList();
			}
			if (others.isEmpty() && which.equals("remote"))
				throw new ConfigError("포털에 원본 외의 리전이 등록돼 있지 않음");
			int i = 1;
			for (PortalRegion region : others) {
				S3Client dst = Common.makeS3Client(region.endpoint(settings.useHttps), region.accessKey(), region.secretKey());
				// 주소가 같아도 포트가 다르면 앞단 haproxy 가 다른 시스템으로 보낼 수 있어 둘 다 같아야 같은 시스템으로 본다
				boolean same = region.address().equals(home.address()) && region.port() == home.port();
				targets.add(new Target("remote", dst, region, i++, same));
			}
		}
		return new Built(home, s3, targets);
	}

	static String describeRegion(PortalRegion region, boolean https) {
		return region.name() + " " + region.endpoint(https);
	}

	/** 설정·포털·리전·키만 점검한다. 버킷은 만들지 않는다. */
	static int check(Settings settings, PortalClient portal, String target, List<String> regionsWanted) {
		Built b;
		try {
			b = buildTargets(settings, portal, target, regionsWanted);
		} catch (CaseFailed | ConfigError e) {
			System.out.println("[FAIL] " + e.getMessage());
			return EXIT_CONFIG;
		}
		boolean ok = true;
		System.out.println("[OK]   포털 " + settings.portalUrl + " (" + settings.portalType + "), 원본 리전 "
				+ describeRegion(b.home(), settings.useHttps) + (settings.homeRegion.isEmpty() ? "" : " ([portal] region)"));
		String[] src = settings.sourceEndpoint(b.home());
		System.out.println("[OK]   원본 S3 " + src[0] + " (" + src[1] + ")");
		List<Object[]> clients = new ArrayList<>();
		clients.add(new Object[] { "원본 사용자 키", b.s3() });
		for (Target t : b.targets())
			if (t.region() != null)
				clients.add(new Object[] { "리전 " + t.region().name() + " 키(" + t.region().endpoint(settings.useHttps) + ")",
						t.s3() });
		for (Object[] c : clients) {
			try {
				int n = ((S3Client) c[1]).listBuckets().buckets().size();
				System.out.println("[OK]   " + c[0] + ": ListBuckets 성공(버킷 " + n + "개)");
			} catch (Exception e) {
				ok = false;
				System.out.println("[FAIL] " + c[0] + ": ListBuckets 실패 " + e.getMessage());
			}
		}
		for (Target t : b.targets())
			if (t.region() != null)
				System.out.println("[INFO] remote 대상 " + t.region().name() + ": "
						+ (t.samePortal() ? "같은 포털, 대상 usage 비교" : "다른 시스템, 대상 usage 건너뜀"));
		if (b.targets().stream().noneMatch(t -> t.region() != null) && !target.equals("local"))
			System.out.println("[INFO] 원본 외 리전이 없어 remote 케이스는 돌지 않음");
		return ok ? EXIT_OK : EXIT_CONFIG;
	}

	static String csv(Object value) {
		String s = value == null ? "" : String.valueOf(value);
		if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r"))
			return "\"" + s.replace("\"", "\"\"") + "\"";
		return s;
	}

	static void appendSummary(Path file, String runTime, List<CaseResult> results) throws IOException {
		StringBuilder sb = new StringBuilder();
		if (!Files.exists(file))
			sb.append(SUMMARY_HEADER).append('\n');
		for (CaseResult r : results) {
			Usage s = r.srcUsage;
			Usage d = r.dstUsage;
			List<Object> row = List.of(runTime, r.caseId, r.result, r.failedStep,
					r.replicationSec == null ? "" : String.format("%.1f", r.replicationSec),
					r.srcBucket, s == null ? "" : s.fileCount(), s == null ? "" : s.usedSize(),
					r.dstBucket, d == null ? "" : d.fileCount(), d == null ? "" : d.usedSize(), r.message);
			sb.append(row.stream().map(Main::csv).collect(Collectors.joining(","))).append('\n');
		}
		Files.writeString(file, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}

	static int run(String[] argv) throws Exception {
		Path base = baseDir();
		Args args;
		try {
			args = parseArgs(argv, base);
		} catch (ConfigError e) {
			System.err.println(e.getMessage());
			return EXIT_CONFIG;
		}

		if (args.list) {
			for (var c : Cases.ALL) {
				ReplicationCase rc = c.get();
				System.out.println(rc.id() + "  " + rc.title());
			}
			return EXIT_OK;
		}

		List<java.util.function.Supplier<ReplicationCase>> selected = Cases.ALL;
		if (!args.tcs.isEmpty()) {
			Set<String> wanted = args.tcs.stream().map(String::toUpperCase).collect(Collectors.toCollection(LinkedHashSet::new));
			selected = Cases.ALL.stream().filter(c -> wanted.contains(c.get().id())).toList();
			Set<String> unknown = new TreeSet<>(wanted);
			selected.forEach(c -> unknown.remove(c.get().id()));
			if (!unknown.isEmpty()) {
				System.err.println("없는 케이스: " + String.join(", ", unknown));
				return EXIT_CONFIG;
			}
		}

		Settings settings;
		try {
			settings = Settings.load(args.config);
		} catch (ConfigError e) {
			System.err.println("설정 오류: " + e.getMessage());
			return EXIT_CONFIG;
		}
		PortalClient portal = new PortalClient(settings);

		if (args.check)
			return check(settings, portal, args.target, args.regions);

		Built b;
		try {
			b = buildTargets(settings, portal, args.target, args.regions);
		} catch (CaseFailed | ConfigError e) {
			System.err.println("설정 오류: " + e.getMessage());
			return EXIT_CONFIG;
		}

		Path resultsDir = base.resolve("results");
		Files.createDirectories(resultsDir);
		String runTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
		Log.toFile(resultsDir.resolve(runTime + ".log"));
		Log.info("실행 시작 %s: portal=%s 원본=%s 대상=%s 파일 %d개 크기 %s", runTime, settings.portalUrl,
				describeRegion(b.home(), settings.useHttps),
				b.targets().stream().map(t -> t.region() == null ? "local" : t.region().name()).collect(Collectors.joining(", ")),
				settings.fileCount, settings.fileSizes.stream().map(Common::formatSize).collect(Collectors.joining(",")));

		if (args.cleanupDays != null) {
			// 원본 클라이언트(원본·local 대상)와 remote 대상 클라이언트마다 한 번씩
			int n = Common.cleanupOldBuckets(b.s3(), args.cleanupDays);
			Log.info("오래된 시험 버킷 정리(%s, %d일 초과): %d개 삭제", "원본", args.cleanupDays, n);
			for (Target t : b.targets()) {
				if (t.region() == null)
					continue;
				n = Common.cleanupOldBuckets(t.s3(), args.cleanupDays);
				Log.info("오래된 시험 버킷 정리(%s, %d일 초과): %d개 삭제", t.region().name(), args.cleanupDays, n);
			}
		}

		List<CaseResult> results = new ArrayList<>();
		for (Target t : b.targets())
			for (var c : selected)
				results.add(c.get().bind(settings, b.s3(), portal, t).execute());
		appendSummary(resultsDir.resolve("summary.csv"), runTime, results);

		List<CaseResult> failed = results.stream().filter(r -> !r.result.equals("PASS")).toList();
		Log.info("실행 종료: %d개 중 통과 %d, 실패 %d", results.size(), results.size() - failed.size(), failed.size());
		for (CaseResult r : failed)
			Log.info("  실패 %s %s: %s", r.caseId, r.failedStep, Json.shorten(r.message, 300));
		return failed.isEmpty() ? EXIT_OK : EXIT_FAIL;
	}

	public static void main(String[] argv) throws Exception {
		// 서버 로케일(LANG=C 등)과 상관없이 한글이 깨지지 않게 화면 출력도 UTF-8 로 고정한다
		System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8));
		System.setErr(new PrintStream(new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8));
		System.exit(run(argv));
	}
}
