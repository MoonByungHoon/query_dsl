package study.querydsl.Entity;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.stereotype.Service;

@Entity
@Getter
@Setter
@ToString(of = {"id", "username", "age"})
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Service
public class Member {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "member_id")
  private Long id;
  private String username;
  private int age;

  @ManyToOne(fetch = FetchType.LAZY)
  @JoinColumn(name = "team_id")
  private Team team;

  public Member(String username, int age, Team team) {
    this.username = username;
    this.age = age;
    if (team != null) {
      changeTeam(team);
    }
  }

  public Member(String username) {
    this(username, 0);
  }

  public Member(String username, int age) {
    this(username, age, null);
  }

  public void changeTeam(Team team) {
    this.team = team;
    team.getMembers().add(this);
  }
}
