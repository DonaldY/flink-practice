package com.donaldy.demo.cep;

/**
 * @author donald
 * @date 2021/04/24
 */
public class PayBean {
    private Long id;
    private String state;
    private Long ts;

    public PayBean() {
    }

    public PayBean(Long id, String state, Long ts) {
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
}
