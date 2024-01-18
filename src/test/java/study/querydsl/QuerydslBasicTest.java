package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.Entity.Member;
import study.querydsl.Entity.QMember;
import study.querydsl.Entity.Team;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.select;
import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.Entity.QMember.member;
import static study.querydsl.Entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

  @Autowired
  private EntityManager em;

  private JPAQueryFactory queryFactory;

  @BeforeEach
  public void before() {
    queryFactory = new JPAQueryFactory(em);

    Team teamA = new Team("teamA");
    Team teamB = new Team("teamB");
    em.persist(teamA);
    em.persist(teamB);

    Member member1 = new Member("member1", 10, teamA);
    Member member2 = new Member("member2", 15, teamA);
    Member member3 = new Member("member3", 20, teamA);
    Member member4 = new Member("member4", 25, teamA);

    Member member5 = new Member("member5", 30, teamB);
    Member member6 = new Member("member6", 35, teamB);
    Member member7 = new Member("member7", 40, teamB);
    Member member8 = new Member("member8", 45, teamB);
    em.persist(member1);
    em.persist(member2);
    em.persist(member3);
    em.persist(member4);
    em.persist(member5);
    em.persist(member6);
    em.persist(member7);
    em.persist(member8);
  }

  @Test
  public void startJPQL() {
    //member1 조회.
    Member findMember1 = em.createQuery("select m from Member m where m.username = :username", Member.class)
            .setParameter("username", "member1")
            .getSingleResult();

    assertThat(findMember1.getUsername()).isEqualTo("member1");
  }

  @Test
  public void startQuerydsl() {
//    QMember m = new QMember("m");
//    QMember m = QMember.member;
    Member findMember1 = queryFactory.selectFrom(member)
            .where(member.username.eq("member1"))
            .fetchOne();

    assertThat(findMember1.getUsername()).isEqualTo("member1");
  }

  @Test
  public void search() {
    Member findMember = queryFactory.selectFrom(member)
            .where(member.username.eq("member1")
                    .and(member.age.eq(10)))
            .fetchOne();

    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertThat(findMember.getAge()).isEqualTo(10);
  }

  @Test
  public void searchAndParam() {
    Member findMember = queryFactory.selectFrom(member)
            .where(member.username.eq("member1"),
                    member.age.eq(10))
            .fetchOne();

    assertThat(findMember.getUsername()).isEqualTo("member1");
    assertThat(findMember.getAge()).isEqualTo(10);
  }

  @Test
  public void resultFetch() {
//    List<Member> fetch = queryFactory
//            .selectFrom(member)
//            .fetch();
//
//    Member fetchOne = queryFactory
//            .selectFrom(member)
//            .fetchOne();
//
//    Member fetchFirst = queryFactory.selectFrom(member)
//            .fetchFirst();

    QueryResults<Member> results = queryFactory
            .selectFrom(member)
            .fetchResults();

    List<Member> content = results.getResults();

    System.out.println("확인 : " + results.getTotal() + "확인 : " + content);

    long total = queryFactory.selectFrom(member)
            .fetchCount();

    System.out.println("total : " + total);
  }

  @Test
  public void sort() {
    //1. 나이 내림차순(desc)
    //2. 이름 올림차순(asc) 단 회원 이름이 없으면 마지막에 출력.(nullsLast())
    List<Member> result = queryFactory
            .selectFrom(member)
            .where(member.age.loe(30))
            .orderBy(member.age.desc(),
                    member.username.asc().nullsLast())
            .fetch();

    for (Member member1 : result) {
      System.out.println("member1.getId() = " + member1.getId());
      System.out.println("member1.getUsername() = " + member1.getUsername());
      System.out.println("member1.getAge() = " + member1.getAge());
    }
  }

  @Test
  public void paging1() {
    List<Member> result = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(1)
            .limit(2)
            .fetch();

    assertThat(result.size()).isEqualTo(2);
  }

  @Test
  public void paging2() {
    QueryResults<Member> queryResults = queryFactory
            .selectFrom(member)
            .orderBy(member.username.desc())
            .offset(1)
            .limit(2)
            .fetchResults();

    assertThat(queryResults.getTotal()).isEqualTo(8);
    assertThat(queryResults.getLimit()).isEqualTo(2);
    assertThat(queryResults.getOffset()).isEqualTo(1);
    assertThat(queryResults.getResults().size()).isEqualTo(2);
  }

  @Test
  public void aggregation() {
    List<Tuple> result = queryFactory
            .select(
                    member.username,
                    member.count(),
                    member.age.sum(),
                    member.age.avg(),
                    member.age.max(),
                    member.age.min()
            )
            .from(member)
            .groupBy(member)
            .fetch();

    List<Tuple> result2 = queryFactory
            .select(member.username,
                    member.age)
            .from(member)
            .fetch();

    for (Tuple tuple : result2) {
      System.out.println("tuple.get(member.username) = " + tuple.get(member.username));
    }

    for (Tuple tuple : result) {
      System.out.println("tuple.get(member.count()) = " + tuple.get(member.count()));
      System.out.println("tuple.get(member.age.sum()) = " + tuple.get(member.age.sum()));
      System.out.println("tuple.get(member.age.avg()) = " + tuple.get(member.age.avg()));
      System.out.println("tuple.get(member.age.max()) = " + tuple.get(member.age.max()));
      System.out.println("tuple.get(member.age.min()) = " + tuple.get(member.age.min()));
      System.out.println("tuple.get(member.username) = " + tuple.get(member.username));
    }
  }

  @Test
  public void group() throws Exception {
    List<Tuple> result = queryFactory
            .select(team.name, member.age.avg())
            .from(member)
            .join(member.team, team)
            .groupBy(team.name)
            .fetch();

    assertThat(result.get(0).get(team.name)).isEqualTo("teamA");
    assertThat(result.get(1).get(team.name)).isEqualTo("teamB");
  }

  @Test
  public void join() {
//    팀 A에 소속된 모든 회원 조회

    List<Member> result = queryFactory.selectFrom(member)
            .join(member.team, team)
            .where(team.name.eq("teamA"))
            .fetch();

    assertThat(result)
            .extracting("username")
            .containsExactly("member1", "member2", "member3", "member4");
  }

  @Test
  public void theta_join() {
//    연관관계가 없는 엔티티 외부조인.
//    회원의 이름이 팀 이름과 같은 대상 외부조인
    List<Member> result = queryFactory
            .select(member)
            .from(member, team)
            .where(member.username.eq(team.name))
            .fetch();
  }

  @Test
  public void join_on_filtering() {
//    회원과 팀을 조인. 팀 이름이 teamA인 팀만. 회원은 전부.

    List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            .leftJoin(member.team, team)
            .on(team.name.eq("teamA"))
//            .where(team.name.eq("teamA"))
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
//      System.out.println("tuple.get(member.username) = " + tuple.get(member.username));
    }
  }

  @Test
  public void join_on_no_relation() {
//    연관관계가 없는 엔티티의 외부조인
//    회원의 이름이 팀 이름과 같은 대상 외부조인
    List<Tuple> result = queryFactory
            .select(member, team)
            .from(member)
            .leftJoin(team)
            .on(member.username.eq(team.name))
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }
  }

  @PersistenceUnit
  EntityManagerFactory emf;

  @Test
  public void fetchJoinNo() {
    em.flush();
    em.clear();

    Member findMember1 = queryFactory
            .selectFrom(member)
            .where(member.username.eq("member1"))
            .fetchOne();

    System.out.println("findMember1 = " + findMember1);

//    로딩이 된 Entity인지 아닌지 영속성 컨텍스트를 확인할 수 있다. 여기서 로딩이란 초기화가 되었는지를 따진다.
    boolean loaded1 = emf.getPersistenceUnitUtil().isLoaded(findMember1.getTeam());

//    페치 조인이 되었다면 값이 초기화가 되기 때문에 영속성 컨텍스트에 엔티티가 등록되기 때문에 True가 나온다.
//    하지만 현재 코드에서는 지연 로딩이 설정 되어있기 때문에 별도로 페치 조인을 요구하지 않아서 false가 뜨게 된다.
//    하지만 중간에 findMember1.getTeam()을 호출하는 순간 쿼리가 나가면서 별도의 Team 조회 쿼리가 나가게 된다.
//    이 점을 조심해야한다.
    assertThat(loaded1).as("첫번째 페치 조인 미적용").isFalse();

    em.flush();
    em.clear();


    Member findMember2 = queryFactory
            .selectFrom(member)
            .join(member.team, team).fetchJoin()
            .where(member.username.eq("member1"))
            .fetchOne();

    boolean loaded2 = emf.getPersistenceUnitUtil().isLoaded(findMember2.getTeam());

//    fetchJoin을 적용해서 즉시 로딩 시켰기 때문에 True가 나오게 된다.
    assertThat(loaded2).as("두번째 페치 조인 미적용").isTrue();

    System.out.println("findMember2 = " + findMember2);
  }

  @Test
  public void subQuery() {
//    나이가 가장 많은 회원 조회

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory.selectFrom(member)
            .where(member.age.eq(
                    select(memberSub.age.max())
                            .from(memberSub)
            )).fetch();

    for (Member member1 : result) {
      System.out.println("member1 = " + member1);
    }
  }

  @Test
  public void subQueryGoe() {
//    나이가 평균 이상인 회원

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory.selectFrom(member)
            .where(member.age.goe(
                    select(memberSub.age.avg())
                            .from(memberSub)
            )).fetch();

    for (Member member1 : result) {
      System.out.println("member1 = " + member1);
    }
  }

  @Test
  public void subQueryIn() {
//    나이가 가장 많은 회원 조회

    QMember memberSub = new QMember("memberSub");

    List<Member> result = queryFactory.selectFrom(member)
            .where(member.age.in(
                    select(memberSub.age)
                            .from(memberSub)
                            .where(member.age.gt(40))
            )).fetch();

    for (Member member1 : result) {
      System.out.println("member1 = " + member1);
    }
  }

  @Test
  public void selectSubQuery() {

    QMember memberSub = new QMember("memberSub");

    List<Tuple> result = queryFactory
            .select(member.username,
                    select(memberSub.age.avg())
                            .from(memberSub))
            .from(memberSub)
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }
  }

  @Test
  public void basicCase() {
    List<String> result = queryFactory
            .select(member.age
                    .when(10).then("열살")
                    .when(20).then("스무살")
                    .when(30).then("서른살")
                    .otherwise("기타"))
            .from(member)
            .fetch();

    for (String string : result) {
      System.out.println("string = " + string);
    }
  }

  @Test
  public void complexCase() {
    List<String> result = queryFactory
            .select(new CaseBuilder()
                    .when(member.age.between(0, 20)).then("0~20살")
                    .when(member.age.between(21, 30)).then("21~30살")
                    .otherwise("31이상"))
            .from(member)
            .fetch();

    for (String string : result) {
      System.out.println("string = " + string);
    }
  }

  @Test
  public void constant() {
    List<Tuple> result = queryFactory
            .select(member.username, Expressions.constant("A"))
            .from(member)
            .fetch();

    for (Tuple tuple : result) {
      System.out.println("tuple = " + tuple);
    }
  }

  @Test
  public void concat() {
    List<String> result = queryFactory
            .select(member.username.concat("_").concat(member.age.stringValue()))
            .from(member)
            .fetch();

    for (String string : result) {
      System.out.println("string = " + string);
    }
  }

  @Test
  public void simpleProjection() {
    List<String> result1 = queryFactory
            .select(member.username)
            .from(member)
            .fetch();

    List<Member> result2 = queryFactory
            .select(member)
            .from(member)
            .fetch();
  }

  @Test
  public void tupleProjection() {
    List<Tuple> result = queryFactory
            .select(member.username, member.age)
            .from(member)
            .fetch();
  }

  @Test
  public void findDtoByJPQL() {
    List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) " +
            "from Member m", MemberDto.class).getResultList();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }

  @Test
  public void findDtoBySetter() {
    List<MemberDto> result = queryFactory
            .select(Projections.bean(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto.getUsername());
    }
  }

  @Test
  public void findDtoByField() {
    List<MemberDto> result = queryFactory
            .select(Projections.fields(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto.getUsername());
    }
  }

  @Test
  public void findDtoByConstructor() {
    List<MemberDto> result = queryFactory
            .select(Projections.constructor(MemberDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto.getUsername());
    }
  }

  @Test
  public void findUserDto() {
    List<UserDto> result = queryFactory
            .select(Projections.fields(UserDto.class,
                    member.username.as("name"),
                    member.age))
            .from(member)
            .fetch();

    for (UserDto userDto : result) {
      System.out.println("userDto = " + userDto);
    }
  }

  @Test
  public void findUserDtoSubByField() {
    QMember memberSub = new QMember("memberSub");

    List<UserDto> result = queryFactory
            .select(Projections.fields(UserDto.class,
                    member.username.as("name"),
                    ExpressionUtils.as(JPAExpressions
                            .select(memberSub.age.min())
                            .from(memberSub), "age")
            ))
            .from(member)
            .fetch();

    for (UserDto userDto : result) {
      System.out.println("userDto = " + userDto);
    }
  }

  @Test
  public void findUserDtoSubByConstructor() {
    QMember memberSub = new QMember("memberSub");

    List<UserDto> result = queryFactory
            .select(Projections.constructor(UserDto.class,
                    member.username,
                    member.age))
            .from(member)
            .fetch();

    for (UserDto userDto : result) {
      System.out.println("userDto = " + userDto);
    }
  }

  @Test
  public void findDtoByQueryProjection() {
    List<MemberDto> result = queryFactory
            .select(new QMemberDto(member.username, member.age, member.id))
            .from(member)
            .fetch();

    for (MemberDto memberDto : result) {
      System.out.println("memberDto = " + memberDto);
    }
  }
}