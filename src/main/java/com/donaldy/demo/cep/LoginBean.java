package com.donaldy.demo.cep;

/**
 * @author donald
 * @date 2021/04/23
 */
public class LoginBean {

    private Long id;
    private String state;
    private Long ts;

    public LoginBean() {

    }

    public LoginBean(Long id, String state, Long ts) {
        this.id = id;
        this.state = state;
        this.ts = ts;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "LoginBean{" +
                "id=" + id +
                ", state='" + state + '\'' +
                ", ts=" + ts +
                '}';
    }
}
